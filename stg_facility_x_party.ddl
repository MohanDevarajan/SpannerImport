  
CREATE TABLE STG_FACILITY_X_PARTY (
    PARTY_FAC_CODE INT64,
    domain PARTY_ID INT64,
    FACILITY_ID INT64,
    Start_date FORMAT 'dd.mm.yyyy',
    End_Date FORMAT 'dd.mm.yyyy',
    Code INT64,
    Descriptor STRING(100),
)
