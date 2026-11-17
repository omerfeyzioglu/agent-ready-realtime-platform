package main

// Trip is one row of source/tlc/fhvhv: a finished trip as the app database
// would hold it. Timestamps are NYC local wall-clock time in microseconds
// (no zone attached), exactly as TLC publishes them.
type Trip struct {
	TripID              [16]byte `parquet:"trip_id,uuid"`
	Provider            *string  `parquet:"hvfhs_license_num,optional"`
	DispatchingBase     *string  `parquet:"dispatching_base_num,optional"`
	OriginatingBase     *string  `parquet:"originating_base_num,optional"`
	RequestAt           *int64   `parquet:"request_datetime,optional"`
	OnSceneAt           *int64   `parquet:"on_scene_datetime,optional"`
	PickupAt            *int64   `parquet:"pickup_datetime,optional"`
	DropoffAt           *int64   `parquet:"dropoff_datetime,optional"`
	PickupZone          *int32   `parquet:"PULocationID,optional"`
	DropoffZone         *int32   `parquet:"DOLocationID,optional"`
	TripMiles           *float64 `parquet:"trip_miles,optional"`
	TripTime            *int64   `parquet:"trip_time,optional"`
	BasePassengerFare   *float64 `parquet:"base_passenger_fare,optional"`
	Tolls               *float64 `parquet:"tolls,optional"`
	BCF                 *float64 `parquet:"bcf,optional"`
	SalesTax            *float64 `parquet:"sales_tax,optional"`
	CongestionSurcharge *float64 `parquet:"congestion_surcharge,optional"`
	AirportFee          *float64 `parquet:"airport_fee,optional"`
	Tips                *float64 `parquet:"tips,optional"`
	DriverPay           *float64 `parquet:"driver_pay,optional"`
	SharedRequest       *string  `parquet:"shared_request_flag,optional"`
	SharedMatch         *string  `parquet:"shared_match_flag,optional"`
	AccessARide         *string  `parquet:"access_a_ride_flag,optional"`
	WavRequest          *string  `parquet:"wav_request_flag,optional"`
	WavMatch            *string  `parquet:"wav_match_flag,optional"`
	CBDCongestionFee    *float64 `parquet:"cbd_congestion_fee,optional"`
}

// The four timestamp columns; row-group statistics of these give the earliest
// event a row group can contain.
var timeColumns = []string{"request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime"}
