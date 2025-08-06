import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Iterator, Tuple
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OAIPMHClient:
    """Client for arXiv OAI-PMH API with robust error handling and rate limiting."""
    
    def __init__(self, base_url: str = "https://oaipmh.arxiv.org/oai"):
        self.base_url = base_url
        self.session = self._create_session()
        
        # OAI-PMH namespaces
        self.namespaces = {
            'oai': 'http://www.openarchives.org/OAI/2.0/',
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'arxiv': 'http://arxiv.org/OAI/arXiv/'
        }
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set user agent (arXiv requests identification)
        email = os.getenv('EMAIL', 'contact-via-github-issues')
        contact_info = f"mailto:{email}" if '@' in email else email
        session.headers.update({
            'User-Agent': f'PaperWeave/1.0 (https://github.com/paperweave/paperweave; {contact_info})'
        })
        
        return session
    
    def _make_request(self, params: Dict) -> ET.Element:
        """Make OAI-PMH request with error handling."""
        try:
            logger.debug(f"Making OAI-PMH request: {params}")
            response = self.session.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)
            
            # Check for OAI-PMH errors
            error_elem = root.find('.//oai:error', self.namespaces)
            if error_elem is not None:
                error_code = error_elem.get('code', 'unknown')
                error_text = error_elem.text or 'No error message'
                raise Exception(f"OAI-PMH Error [{error_code}]: {error_text}")
            
            return root
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except ET.ParseError as e:
            logger.error(f"XML parsing failed: {e}")
            raise
    
    def identify(self) -> Dict:
        """Get repository information."""
        params = {'verb': 'Identify'}
        root = self._make_request(params)
        
        identify_elem = root.find('.//oai:Identify', self.namespaces)
        if identify_elem is None:
            raise Exception("Invalid Identify response")
        
        result = {}
        for child in identify_elem:
            tag = child.tag.replace('{http://www.openarchives.org/OAI/2.0/}', '')
            result[tag] = child.text
        
        return result
    
    def list_metadata_formats(self) -> List[Dict]:
        """Get supported metadata formats."""
        params = {'verb': 'ListMetadataFormats'}
        root = self._make_request(params)
        
        formats = []
        for format_elem in root.findall('.//oai:metadataFormat', self.namespaces):
            format_info = {}
            for child in format_elem:
                tag = child.tag.replace('{http://www.openarchives.org/OAI/2.0/}', '')
                format_info[tag] = child.text
            formats.append(format_info)
        
        return formats
    
    def list_sets(self) -> List[Dict]:
        """Get available sets."""
        params = {'verb': 'ListSets'}
        root = self._make_request(params)
        
        sets = []
        for set_elem in root.findall('.//oai:set', self.namespaces):
            set_info = {}
            for child in set_elem:
                tag = child.tag.replace('{http://www.openarchives.org/OAI/2.0/}', '')
                set_info[tag] = child.text
            sets.append(set_info)
        
        return sets
    
    def list_records(self, 
                    metadata_prefix: str = 'oai_dc',
                    from_date: Optional[datetime] = None,
                    until_date: Optional[datetime] = None,
                    set_spec: Optional[str] = None,
                    resumption_token: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        """
        List records with optional filtering.
        
        Returns:
            Tuple of (records, resumption_token)
        """
        params = {'verb': 'ListRecords'}
        
        if resumption_token:
            params['resumptionToken'] = resumption_token
        else:
            params['metadataPrefix'] = metadata_prefix
            if from_date:
                params['from'] = from_date.strftime('%Y-%m-%d')
            if until_date:
                params['until'] = until_date.strftime('%Y-%m-%d')
            if set_spec:
                params['set'] = set_spec
        
        root = self._make_request(params)
        
        # Extract records
        records = []
        for record_elem in root.findall('.//oai:record', self.namespaces):
            record = self._parse_record(record_elem)
            if record:
                records.append(record)
        
        # Check for resumption token
        resumption_elem = root.find('.//oai:resumptionToken', self.namespaces)
        next_token = resumption_elem.text if resumption_elem is not None else None
        
        return records, next_token
    
    def _parse_record(self, record_elem: ET.Element) -> Optional[Dict]:
        """Parse a single OAI-PMH record."""
        try:
            header_elem = record_elem.find('oai:header', self.namespaces)
            if header_elem is None:
                return None
            
            # Check if record is deleted
            if header_elem.get('status') == 'deleted':
                return {
                    'identifier': header_elem.find('oai:identifier', self.namespaces).text,
                    'datestamp': header_elem.find('oai:datestamp', self.namespaces).text,
                    'status': 'deleted'
                }
            
            metadata_elem = record_elem.find('oai:metadata', self.namespaces)
            if metadata_elem is None:
                return None
            
            # Parse metadata based on format
            dc_elem = metadata_elem.find('oai_dc:dc', self.namespaces)
            if dc_elem is not None:
                return self._parse_dublin_core(header_elem, dc_elem)
            
            # Could add other metadata format parsers here
            return None
            
        except Exception as e:
            logger.warning(f"Failed to parse record: {e}")
            return None
    
    def _parse_dublin_core(self, header_elem: ET.Element, dc_elem: ET.Element) -> Dict:
        """Parse Dublin Core metadata."""
        record = {
            'identifier': header_elem.find('oai:identifier', self.namespaces).text,
            'datestamp': header_elem.find('oai:datestamp', self.namespaces).text,
            'status': 'active'
        }
        
        # Extract arXiv ID from identifier (e.g., oai:arXiv.org:1234.5678)
        identifier = record['identifier']
        if ':' in identifier:
            arxiv_id = identifier.split(':')[-1]
            record['arxiv_id'] = arxiv_id
        
        # Parse Dublin Core elements
        for child in dc_elem:
            tag = child.tag.replace('{http://purl.org/dc/elements/1.1/}', '')
            
            if tag in record:
                # Handle multiple values
                if isinstance(record[tag], list):
                    record[tag].append(child.text)
                else:
                    record[tag] = [record[tag], child.text]
            else:
                record[tag] = child.text
        
        # Parse sets (subjects/categories)
        sets = []
        for set_elem in header_elem.findall('oai:setSpec', self.namespaces):
            sets.append(set_elem.text)
        record['sets'] = sets
        
        return record
    
    def harvest_incremental(self, 
                          last_update: datetime,
                          metadata_prefix: str = 'oai_dc',
                          batch_size: int = 1000) -> Iterator[List[Dict]]:
        """
        Harvest records incrementally since last update.
        
        Yields batches of records.
        """
        logger.info(f"Starting incremental harvest from {last_update}")
        
        total_records = 0
        resumption_token = None
        request_count = 0
        
        while True:
            try:
                # arXiv fair use policy: 4 requests per second with 1 second sleep per burst
                if request_count > 0 and request_count % 4 == 0:
                    logger.debug("Rate limiting: 1 second sleep after 4 requests")
                    time.sleep(1.0)
                
                records, resumption_token = self.list_records(
                    metadata_prefix=metadata_prefix,
                    from_date=last_update,
                    resumption_token=resumption_token
                )
                
                request_count += 1
                
                if not records:
                    break
                
                total_records += len(records)
                logger.info(f"Harvested {len(records)} records (total: {total_records})")
                
                yield records
                
                if not resumption_token:
                    break
                    
            except Exception as e:
                logger.error(f"Error during harvest: {e}")
                # Wait before retrying
                time.sleep(60)
                continue
        
        logger.info(f"Incremental harvest completed. Total records: {total_records}")


def test_oai_pmh_client():
    """Test the OAI-PMH client with arXiv."""
    client = OAIPMHClient()
    
    # Test identify
    try:
        info = client.identify()
        logger.info(f"Repository: {info.get('repositoryName')}")
        logger.info(f"Base URL: {info.get('baseURL')}")
        logger.info(f"Protocol Version: {info.get('protocolVersion')}")
    except Exception as e:
        logger.error(f"Identify failed: {e}")
        return
    
    # Test metadata formats
    try:
        formats = client.list_metadata_formats()
        logger.info(f"Available metadata formats: {[f.get('metadataPrefix') for f in formats]}")
    except Exception as e:
        logger.error(f"ListMetadataFormats failed: {e}")
    
    # Test small harvest
    try:
        yesterday = datetime.now() - timedelta(days=1)
        for batch in client.harvest_incremental(yesterday):
            logger.info(f"Sample batch: {len(batch)} records")
            if batch:
                logger.info(f"First record: {batch[0].get('arxiv_id')} - {batch[0].get('title', 'No title')}")
            break  # Only test first batch
    except Exception as e:
        logger.error(f"Harvest test failed: {e}")


if __name__ == "__main__":
    test_oai_pmh_client()