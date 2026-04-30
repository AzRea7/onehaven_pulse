from pipelines.extractors.fhfa.config import FHFA_HPI_MASTER


def test_fhfa_hpi_master_config():
    assert FHFA_HPI_MASTER.dataset == "hpi"
    assert FHFA_HPI_MASTER.filename == "hpi_master.csv"
    assert FHFA_HPI_MASTER.url.startswith("https://www.fhfa.gov/")
    assert FHFA_HPI_MASTER.expected_frequency == "monthly"
