from dataclasses import dataclass


@dataclass(frozen=True)
class TableDef:
    """Definition of a SCDM table schema."""

    name: str
    columns: tuple[str, ...]
    sort_keys: tuple[str, ...]
    crosswalk_ids: dict[str, str]


TABLES = {
    "enrollment": TableDef(
        name="enrollment",
        columns=(
            "PatID",
            "Enr_Start",
            "Enr_End",
            "MedCov",
            "DrugCov",
            "Chart",
            "PlanType",
            "PayerType",
        ),
        sort_keys=("PatID", "Enr_Start", "Enr_End", "MedCov", "DrugCov", "Chart"),
        crosswalk_ids={"PatID": "inner"},
    ),
    "demographic": TableDef(
        name="demographic",
        columns=(
            "PatID",
            "Birth_Date",
            "Sex",
            "Hispanic",
            "Race",
            "PostalCode",
            "PostalCode_Date",
            "ImputedRace",
            "ImputedHispanic",
        ),
        sort_keys=("PatID",),
        crosswalk_ids={"PatID": "inner"},
    ),
    "dispensing": TableDef(
        name="dispensing",
        columns=("PatID", "ProviderID", "RxDate", "Rx", "Rx_CodeType", "RxSup", "RxAmt"),
        sort_keys=("PatID", "RxDate"),
        crosswalk_ids={"PatID": "inner", "ProviderID": "left"},
    ),
    "encounter": TableDef(
        name="encounter",
        columns=(
            "PatID",
            "EncounterID",
            "ADate",
            "DDate",
            "EncType",
            "FacilityID",
            "Discharge_Disposition",
            "Discharge_Status",
            "DRG",
            "DRG_Type",
            "Admitting_Source",
        ),
        sort_keys=("PatID", "ADate"),
        crosswalk_ids={"PatID": "inner", "EncounterID": "left", "FacilityID": "left"},
    ),
    "diagnosis": TableDef(
        name="diagnosis",
        columns=(
            "PatID",
            "EncounterID",
            "ADate",
            "ProviderID",
            "EncType",
            "DX",
            "Dx_Codetype",
            "OrigDX",
            "PDX",
            "PAdmit",
        ),
        sort_keys=("PatID", "ADate"),
        crosswalk_ids={"PatID": "inner", "EncounterID": "left", "ProviderID": "left"},
    ),
    "procedure": TableDef(
        name="procedure",
        columns=(
            "PatID",
            "EncounterID",
            "ADate",
            "ProviderID",
            "EncType",
            "PX",
            "PX_CodeType",
            "OrigPX",
        ),
        sort_keys=("PatID", "ADate"),
        crosswalk_ids={"PatID": "inner", "EncounterID": "left", "ProviderID": "left"},
    ),
    "death": TableDef(
        name="death",
        columns=("PatID", "DeathDt", "DtImpute", "Source", "Confidence"),
        sort_keys=("PatID",),
        crosswalk_ids={"PatID": "inner"},
    ),
    "provider": TableDef(
        name="provider",
        columns=("ProviderID", "Specialty", "Specialty_CodeType"),
        sort_keys=("ProviderID",),
        crosswalk_ids={},
    ),
    "facility": TableDef(
        name="facility",
        columns=("FacilityID", "Facility_Location"),
        sort_keys=("FacilityID",),
        crosswalk_ids={},
    ),
}

SOURCE_FILE_EXTENSION = ".sas7bdat"
