\set day_timezone 'UTC'

CREATE TABLE IF NOT EXISTS daily_metrics (
	site_id VARCHAR(100) NOT NULL REFERENCES site(site_id),
	metric_date DATE NOT NULL,
	allergen_type VARCHAR(50) NOT NULL,
	pcount DOUBLE PRECISION NOT NULL CHECK (pcount >= 0),
	cubic_meters DOUBLE PRECISION NOT NULL CHECK (cubic_meters > 0),
	concentration DOUBLE PRECISION NOT NULL CHECK (concentration >= 0),
	n_hours INTEGER NOT NULL CHECK (n_hours >= 1 AND n_hours <= 24),
	n_flow_measurements INTEGER NOT NULL CHECK (n_flow_measurements >= 1),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (site_id, metric_date, allergen_type)
);

ALTER TABLE daily_metrics
	ADD COLUMN IF NOT EXISTS n_flow_measurements INTEGER;

UPDATE daily_metrics
	SET n_flow_measurements = n_hours
	WHERE n_flow_measurements IS NULL;

ALTER TABLE daily_metrics
	ALTER COLUMN n_flow_measurements SET NOT NULL;

ALTER TABLE daily_metrics
	DROP CONSTRAINT IF EXISTS daily_metrics_n_flow_measurements_check;

ALTER TABLE daily_metrics
	ADD CONSTRAINT daily_metrics_n_flow_measurements_check
	CHECK (n_flow_measurements >= 1);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_site_date
	ON daily_metrics (site_id, metric_date);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_allergen
	ON daily_metrics (allergen_type);

WITH category_matches AS (
	SELECT category_map.allergen_type, c.category_id
	FROM (
		VALUES
			('Total Pollen', 'POL'),
			('Total Tree Pollen', 'TRE'),
			('Quercus (Oak)', 'QUE'),
			('Cupressaceae (Cypress)', 'CUP'),
			('Morus (Mulberry)', 'MOR'),
			('Ulmus (Elm)', 'ULM'),
			('Fraxinus (Ash)', 'FRA'),
			('Betula (Birch)', 'BET'),
			('Acer (Maple)', 'ACE'),
			('Populus (Poplar)', 'POP'),
			('Pinaceae (Pine)', 'PIN'),
			('Total Grass Pollen', 'GRA'),
			('Ambrosia (Ragweed)', 'AMB-IVA'),
			('Poaceae (Grasses)', 'POA'),
			('Total Mold', 'MOL')
	) AS category_map(allergen_type, category_code)
	JOIN category c
	  ON c.name = category_map.category_code
),
allergen_types AS (
	SELECT DISTINCT allergen_type
	FROM category_matches
),
flow_hours AS (
	SELECT
		f.site_id,
		f.sensor_id,
		f.moment,
		(f.moment AT TIME ZONE :'day_timezone')::date AS metric_date,
		f.cubic_meters
	FROM hourly_flow f
	WHERE f.cubic_meters > 0
),
hourly_allergen AS (
	SELECT
		m.sensor_id,
		m.moment,
		cm.allergen_type,
		SUM(m.pcount) AS pcount
	FROM hourly_metrics m
	JOIN category_matches cm
	  ON cm.category_id = m.category_id
	GROUP BY m.sensor_id, m.moment, cm.allergen_type
),
daily AS (
	SELECT
		fh.site_id,
		fh.metric_date,
		at.allergen_type,
		SUM(COALESCE(ha.pcount, 0)) AS pcount,
		SUM(fh.cubic_meters) AS cubic_meters,
		SUM(COALESCE(ha.pcount, 0)) / SUM(fh.cubic_meters) AS concentration,
		COUNT(DISTINCT fh.moment) AS n_hours,
		COUNT(*) AS n_flow_measurements
	FROM flow_hours fh
	CROSS JOIN allergen_types at
	LEFT JOIN hourly_allergen ha
	  ON ha.sensor_id = fh.sensor_id
	 AND ha.moment = fh.moment
	 AND ha.allergen_type = at.allergen_type
	GROUP BY fh.site_id, fh.metric_date, at.allergen_type
	HAVING SUM(fh.cubic_meters) > 0
)
INSERT INTO daily_metrics (
	site_id,
	metric_date,
	allergen_type,
	pcount,
	cubic_meters,
	concentration,
	n_hours,
	n_flow_measurements
)
SELECT
	site_id,
	metric_date,
	allergen_type,
	pcount,
	cubic_meters,
	concentration,
	n_hours,
	n_flow_measurements
FROM daily
ON CONFLICT (site_id, metric_date, allergen_type)
DO UPDATE SET
	pcount = EXCLUDED.pcount,
	cubic_meters = EXCLUDED.cubic_meters,
	concentration = EXCLUDED.concentration,
	n_hours = EXCLUDED.n_hours,
	n_flow_measurements = EXCLUDED.n_flow_measurements,
	updated_at = NOW();
