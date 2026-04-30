from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class HmdaDataset:
    dataset: str
    filename: str
    description: str
    expected_frequency: str
    year: int
    geography_type: str
    geography_values: str
    actions_taken: str
    loan_purposes: str
    loan_types: str
    lien_statuses: str


HMDA_MODIFIED_LAR = HmdaDataset(
    dataset="modified_lar",
    filename=(
        f"hmda_modified_lar_{settings.hmda_year}_"
        f"{settings.hmda_geography_type}_{settings.hmda_geography_values.replace(',', '_')}.csv"
    ),
    description="HMDA Data Browser filtered raw CSV download",
    expected_frequency="annual",
    year=settings.hmda_year,
    geography_type=settings.hmda_geography_type,
    geography_values=settings.hmda_geography_values,
    actions_taken=settings.hmda_actions_taken,
    loan_purposes=settings.hmda_loan_purposes,
    loan_types=settings.hmda_loan_types,
    lien_statuses=settings.hmda_lien_statuses,
)
