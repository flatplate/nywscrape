from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class RawArticle:
    """
    This class defines the properties of an unprocessed article
    scraped by newsplease. Not all properties available are mapped
    to this class, since we don't need them all.
    """
    id: int
    date_published: datetime
    source_domain: str
    title: str
    maintext: str
    description: str
    image_url: str
    authors: List[str]
    language: str
