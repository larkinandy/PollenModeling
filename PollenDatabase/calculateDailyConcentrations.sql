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
	SELECT 'Total Pollen' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, group_code, root_group_code, description, common_name)) NOT LIKE '%mold%'
	  AND lower(concat_ws(' ', name, group_code, root_group_code, description, common_name)) NOT LIKE '%fung%'

	UNION ALL
	SELECT 'Total Tree Pollen' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', group_code, root_group_code, description, common_name)) LIKE '%tree%'
	   OR lower(root_group_code) IN ('tree', 'trees', 'tree_pollen')

	UNION ALL
	SELECT 'Quercus (Oak)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%quercus%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%oak%'

	UNION ALL
	SELECT 'Cupressaceae (Cypress)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%cupressaceae%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%cypress%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%cedar%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%juniper%'

	UNION ALL
	SELECT 'Morus (Mulberry)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%morus%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%mulberry%'

	UNION ALL
	SELECT 'Ulmus (Elm)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%ulmus%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%elm%'

	UNION ALL
	SELECT 'Fraxinus (Ash)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%fraxinus%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%ash%'

	UNION ALL
	SELECT 'Betula (Birch)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%betula%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%birch%'

	UNION ALL
	SELECT 'Acer (Maple)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%acer%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%maple%'

	UNION ALL
	SELECT 'Populus (Poplar)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%populus%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%poplar%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%cottonwood%'

	UNION ALL
	SELECT 'Pinaceae (Pine)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%pinaceae%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%pine%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%pinus%'

	UNION ALL
	SELECT 'Total Grass Pollen' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', group_code, root_group_code, description, common_name)) LIKE '%grass%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%poaceae%'

	UNION ALL
	SELECT 'Ambrosia (Ragweed)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%ambrosia%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%ragweed%'

	UNION ALL
	SELECT 'Poaceae (Grasses)' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, description, common_name)) LIKE '%poaceae%'
	   OR lower(concat_ws(' ', name, description, common_name)) LIKE '%grasses%'

	UNION ALL
	SELECT 'Total Mold' AS allergen_type, category_id
	FROM category
	WHERE lower(concat_ws(' ', name, group_code, root_group_code, description, common_name)) LIKE '%mold%'
	   OR lower(concat_ws(' ', name, group_code, root_group_code, description, common_name)) LIKE '%fung%'
),
hourly_allergen AS (
	SELECT
		f.site_id,
		(m.moment AT TIME ZONE :'day_timezone')::date AS metric_date,
		cm.allergen_type,
		m.sensor_id,
		m.moment,
		SUM(m.pcount) AS pcount,
		MAX(f.cubic_meters) AS cubic_meters
	FROM hourly_metrics m
	JOIN hourly_flow f
	  ON f.sensor_id = m.sensor_id
	 AND f.moment = m.moment
	JOIN category_matches cm
	  ON cm.category_id = m.category_id
	GROUP BY f.site_id, (m.moment AT TIME ZONE :'day_timezone')::date, cm.allergen_type, m.sensor_id, m.moment
),
daily AS (
	SELECT
		site_id,
		metric_date,
		allergen_type,
		SUM(pcount) AS pcount,
		SUM(cubic_meters) AS cubic_meters,
		SUM(pcount) / SUM(cubic_meters) AS concentration,
		COUNT(DISTINCT moment) AS n_hours,
		COUNT(*) FILTER (WHERE cubic_meters > 0) AS n_flow_measurements
	FROM hourly_allergen
	GROUP BY site_id, metric_date, allergen_type
	HAVING SUM(cubic_meters) > 0
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
