
from dataclasses import dataclass


@dataclass
class Ontology:
    name: str 
    definition: str 
    description: str

    # these are the list of other terms that are synonyms for this term
    aliases: set[str]



