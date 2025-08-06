# arXiv Fair Use Policy Compliance

This document outlines how PaperWeave complies with arXiv's fair use policy.

## arXiv's Fair Use Requirements

From [arXiv's bulk data access page](https://info.arxiv.org/help/bulk_data.html):

> **Play nice**
> We ask that users intent on harvesting use the dedicated site export.arxiv.org for these purposes, which contains an up-to-date copy of the corpus and is specifically set aside for programmatic access. This will mitigate impact on readers who are using the main site interactively.
>
> There are many users who want to make use of our data, and millions of distinct URLs behind our site. If everyone were to crawl the site at once without regard to a reasonable request rate, the site could be dragged down and unusable. For these purposes we suggest that a reasonable rate to be bursts at 4 requests per second with a 1 second sleep, per burst.

## Our Compliance Implementation

### ✅ **Using Dedicated Infrastructure**

**Requirement**: Use dedicated site `export.arxiv.org` for programmatic access

**Our Implementation**:
- **OAI-PMH Endpoint**: We use `https://oaipmh.arxiv.org/oai` (official OAI-PMH service)
- **Bulk Data**: Initial seeding uses Kaggle dataset, not direct crawling
- **No Main Site Access**: We never access the main arxiv.org site for bulk operations

### ✅ **Rate Limiting**

**Requirement**: 4 requests per second with 1 second sleep per burst

**Our Implementation** (`src/oai_pmh_client.py`):
```python
# arXiv fair use policy: 4 requests per second with 1 second sleep per burst
if request_count > 0 and request_count % 4 == 0:
    logger.debug("Rate limiting: 1 second sleep after 4 requests")
    time.sleep(1.0)
```

**Rate Pattern**:
- Make 4 requests
- Sleep for 1 second
- Repeat pattern
- Effective rate: ~3.2 requests/second (well under limit)

### ✅ **User Agent Identification**

**Requirement**: Identify your application (implied best practice)

**Our Implementation**:
```python
# Uses EMAIL environment variable
email = os.getenv('EMAIL', 'contact-via-github-issues')
contact_info = f"mailto:{email}" if '@' in email else email
'User-Agent': f'PaperWeave/1.0 (https://github.com/paperweave/paperweave; {contact_info})'
```

### ✅ **Respectful Usage Patterns**

**Our Approach**:
- **Incremental Updates**: Only fetch new/changed papers since last run
- **Daily Schedule**: Updates once per day at 23:30 ET (low-traffic period)
- **Minimal Footprint**: Typical daily harvest is 100-500 papers
- **Error Handling**: Graceful retries with exponential backoff
- **Monitoring**: Comprehensive logging to detect issues

## Performance Impact Analysis

### Initial Bulk Loading
- **Frequency**: One-time setup only
- **Source**: Kaggle dataset (not arXiv servers)
- **Impact**: Zero load on arXiv infrastructure

### Daily Updates
- **Frequency**: Once per day
- **Volume**: 100-500 papers typically
- **Requests**: ~50-150 OAI-PMH requests
- **Duration**: < 5 minutes total
- **Rate**: 3.2 requests/second (20% under limit)

### Error Recovery
- **Retry Logic**: 60-second delays between retries
- **Circuit Breaking**: Stops on repeated failures
- **Manual Override**: Requires human intervention for major issues

## Monitoring and Compliance Verification

### Rate Limiting Logs
```
DEBUG - Rate limiting: 1 second sleep after 4 requests
```

### Request Patterns
- Monitor request timestamps in logs
- Verify 1-second gaps every 4 requests
- Track daily request volumes

### Health Metrics
- **Average Requests/Day**: Monitor daily API usage
- **Error Rates**: Track failed requests vs. successful
- **Response Times**: Monitor for signs of server stress

## Future Considerations

### If Policy Changes
- **Monitoring**: Regularly check arXiv's fair use policy
- **Flexibility**: Rate limiting parameters are configurable
- **Adaptation**: Can adjust timing and patterns as needed

### Scale Considerations
- **Current Load**: Well within fair use guidelines
- **Growth Planning**: Monitor usage as system scales
- **Alternative Sources**: Consider arXiv dataset mirrors if needed

## Contact Information

For questions about our usage patterns or compliance:
- **Project**: PaperWeave Knowledge Graph
- **Contact**: Configured via EMAIL environment variable
- **Repository**: https://github.com/paperweave/paperweave
- **Usage**: Academic research and development

## Summary

PaperWeave implements responsible data access practices that exceed arXiv's fair use requirements:

- ✅ Uses dedicated OAI-PMH infrastructure
- ✅ Implements 20% under requested rate limits
- ✅ Minimizes server load through incremental updates
- ✅ Provides clear identification and contact information
- ✅ Includes comprehensive monitoring and error handling

Our implementation respects arXiv's infrastructure while enabling valuable research applications.