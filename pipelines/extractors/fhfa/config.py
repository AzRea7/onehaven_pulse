from dataclasses import dataclass


@dataclass(frozen=True)
class FhfaDataset:
    dataset: str
    url: str
    filename: str
    description: str
    expected_frequency: str


FHFA_HPI_MASTER = FhfaDataset(
    dataset="hpi",
    url="https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv",
    filename="hpi_master.csv",
    description="FHFA House Price Index master CSV",
    expected_frequency="monthly",
)
