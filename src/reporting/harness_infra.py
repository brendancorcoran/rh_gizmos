from dataclasses import dataclass
from enum import Enum
from typing import Optional

from reporting.mapping_query_library import gics_sector_keyword_mappings_query, \
    gov_publication_documents_content_query, aggregator_billdocument_sector_keyword_5in3, \
    gics_subindustry_keyword_mappings_query, aggregator_billdocument_subindustry_keyword_5in3, \
    government_publications_title_query, government_publications_summary_text_query, \
    gov_publication_titles_title_content_query, discovered_alerts_sector_keyword_title_scaled, \
    discovered_alerts_subindustry_keyword_title_scaled, discovered_alerts_extractions


class Impact(Enum):
    DIRECT = "DIRECT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

@dataclass(frozen=True)
class LoadingsQueryStructure:
    name: str
    description: str
    impact: Impact
    loadings_query: callable
    search_terms_query: Optional[callable]
    content_search_target_queries: Optional[list[callable]]

    def __hash__(self):
        return hash(self.name)


# constants
AGGREGATOR_DOCUMENT_XML_SECTOR_KEYWORD_5IN3 = LoadingsQueryStructure(
    name="AGG_XML_SECTOR_KW_5IN3",
    description="Aggregator - Bill Document XML - Sector Keyword - 5 in 3",
    impact=Impact.LOW,
    loadings_query=aggregator_billdocument_sector_keyword_5in3,
    search_terms_query=gics_sector_keyword_mappings_query,
    content_search_target_queries=[gov_publication_documents_content_query],
)

AGGREGATOR_DOCUMENT_XML_SUBINDUSTRY_KEYWORD_5IN3 = LoadingsQueryStructure(
    name="AGG_XML_SUBINDUSTRY_KW_5IN3",
    description="Aggregator - Bill Document XML - Subindustry Keyword - 5 in 3",
    impact=Impact.LOW,
    loadings_query=aggregator_billdocument_subindustry_keyword_5in3,
    search_terms_query=gics_subindustry_keyword_mappings_query,
    content_search_target_queries=[gov_publication_documents_content_query],
)

DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_TITLE_SECTOR_KEYWORD_SCALED = LoadingsQueryStructure(
    name="DA_SECTOR_KW_TITLE_SCALED",
    description="Discovered Alerts - Title - Sector Keyword - Scaled",
    impact=Impact.HIGH,
    loadings_query=discovered_alerts_sector_keyword_title_scaled,
    search_terms_query=gics_sector_keyword_mappings_query,
    content_search_target_queries=[government_publications_title_query,
                                   government_publications_summary_text_query,
                                   gov_publication_titles_title_content_query],
)

DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_TITLE_SUBINDUSTRY_KEYWORD_SCALED = LoadingsQueryStructure(
    name="DA_SUBINDUSTRY_KW_TITLE_SCALED",
    description="Discovered Alerts - Title - Subindustry Keyword - Scaled",
    impact=Impact.HIGH,
    loadings_query=discovered_alerts_subindustry_keyword_title_scaled,
    search_terms_query=gics_subindustry_keyword_mappings_query,
    content_search_target_queries=[government_publications_title_query,
                                   government_publications_summary_text_query,
                                   gov_publication_titles_title_content_query],
)

DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_WATSON_EXTRACTIONS = LoadingsQueryStructure(
    name="DA_EXTRACTIONS",
    description="Discovered Alerts - Government Publications - Watson Extractions",
    impact=Impact.HIGH,
    loadings_query=discovered_alerts_extractions,
    search_terms_query=None,
    content_search_target_queries=None,
)

