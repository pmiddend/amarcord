import urllib.request


def validate_uniprot(uniprot_id: str) -> bool:
    try:
        with urllib.request.urlopen(
            f"https://www.uniprot.org/uniprot/{uniprot_id}.fasta"
        ):
            return True
    except:
        return False
