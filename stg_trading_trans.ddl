CREATE TABLE stg_trading_trans (
	KEY_VALUE INT64,
	ARR_ID INT64,
	Snapshot_DATE DATE,
	Snapshot_Date_ID INT64,
	Limit_Value FLOAT64,
	Limit_GBP FLOAT64,
	Balance	FLOAT64,
	Balance_GBP FLOAT64,
	BI_INSERT_DATE DATE,
	BI_UPDATE_DATE DATE,
	Undrawn_amount FLOAT64,
	Undrawn_amount_GBP FLOAT64,
	Cleared_Balance FLOAT64,
	Cleared_Balance_GBP FLOAT64,
) PRIMARY KEY (KEY_VALUE)