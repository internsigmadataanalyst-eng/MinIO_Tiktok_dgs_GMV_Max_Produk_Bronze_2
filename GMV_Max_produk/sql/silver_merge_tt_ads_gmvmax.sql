MERGE INTO database-sigma.Testing.silver_tt_ads_gmvmax T
USING (

  WITH latest_raw AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        b.*,
        ROW_NUMBER() OVER (
          PARTITION BY
            UPPER(TRIM(b.toko)),
            UPPER(TRIM(b.id_campaign)),
            UPPER(TRIM(b.id_produk)),
            UPPER(TRIM(COALESCE(b.id_video,''))),
            DATE(b.tanggal)
          ORDER BY b.snapshot_ts DESC, b.run_id DESC
        ) rn
      FROM database-sigma.Testing.bronze_maxp b
    )
    WHERE rn = 1
  ),

  scaling_rule AS (
    SELECT
      UPPER(TRIM(toko)) AS toko,
      start_date,
      COALESCE(end_date, DATE '9999-12-31') AS end_date,
      scale_factor
    FROM database-sigma.CONFIG_DB.config_gmvmax_scaling
  ),

  base AS (
    SELECT
      DATE(lr.tanggal)                  AS tanggal,
      UPPER(TRIM(lr.toko))              AS toko,
      UPPER(TRIM(lr.nama_kampanye))     AS nama_kampanye,
      UPPER(TRIM(lr.id_campaign))       AS id_campaign,
      UPPER(TRIM(lr.id_produk))         AS id_produk,
      UPPER(TRIM(lr.jenis_materi_iklan)) AS jenis_materi_iklan,
      UPPER(TRIM(lr.judul_video))       AS judul_video,
      UPPER(TRIM(lr.id_video))          AS id_video,
      UPPER(TRIM(lr.akun_tiktok))       AS akun_tiktok,
      UPPER(TRIM(lr.status))            AS status,
      UPPER(TRIM(lr.jenis_otorisasi))   AS jenis_otorisasi,
      UPPER(TRIM(lr.mata_uang))         AS mata_uang,

      SAFE_CAST(lr.biaya AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS spend,

      SAFE_CAST(lr.pesanan_sku AS INT64) AS orders_sku,

      SAFE_CAST(lr.biaya_per_pesanan AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS cpo,

      SAFE_CAST(lr.pendapatan_kotor AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS revenue_gross,

      SAFE_CAST(REGEXP_REPLACE(lr.roi, r'[^0-9.\-]', '') AS FLOAT64) AS roi,

      SAFE_CAST(lr.impresi_iklan_produk AS INT64)     AS impressions,
      SAFE_CAST(lr.jumlah_klik_iklan_produk AS INT64) AS clicks,

      SAFE_CAST(REGEXP_REPLACE(lr.tingkat_klik_iklan_produk, r'[%\s]', '') AS FLOAT64)/100 AS ctr,
      SAFE_CAST(REGEXP_REPLACE(lr.rasio_konversi_iklan, r'[%\s]', '') AS FLOAT64)/100 AS cvr,

      SAFE_CAST(
        REPLACE(
          REGEXP_REPLACE(lr.rasio_tayang_video_iklan_2_detik, r'[^0-9,.\-]', ''), ',', '.'
          ) AS FLOAT64) / 100 AS vtr_2s,
      SAFE_CAST(
        REPLACE(
          REGEXP_REPLACE(lr.rasio_tayang_video_iklan_6_detik, r'[^0-9,.\-]', ''), ',', '.'
          ) AS FLOAT64) / 100 AS vtr_6s,
      SAFE_CAST(
        REPLACE(
          REGEXP_REPLACE(lr.rasio_tayang_video_iklan_25, r'[^0-9,.\-]', ''), ',', '.'
          ) AS FLOAT64) / 100 AS vtr_25,
      SAFE_CAST(
        REPLACE(
          REGEXP_REPLACE(lr.rasio_tayang_video_iklan_50, r'[^0-9,.\-]', ''), ',', '.'
          ) AS FLOAT64) / 100 AS vtr_50,
      SAFE_CAST(
        REPLACE(
          REGEXP_REPLACE(lr.rasio_tayang_video_iklan_75, r'[^0-9,.\-]', ''), ',', '.'
          ) AS FLOAT64) / 100 AS vtr_75,
      SAFE_CAST(
        REPLACE(
          REGEXP_REPLACE(lr.rasio_tayang_video_iklan_100, r'[^0-9,.\-]', ''), ',', '.'
          ) AS FLOAT64) / 100 AS vtr_100,

      lr.snapshot_ts,
      lr.snapshot_date,
      lr.run_id,
      lr.row_hash_raw

    FROM latest_raw lr
    LEFT JOIN scaling_rule sr
      ON UPPER(TRIM(lr.toko)) = sr.toko
      AND DATE(lr.tanggal) BETWEEN sr.start_date AND sr.end_date
  ),

  with_hash AS (
    SELECT
      b.*,
      TO_HEX(SHA256(
        ARRAY_TO_STRING([
          FORMAT_DATE('%F', b.tanggal),
          b.toko,
          b.id_campaign,
          COALESCE(b.id_video,''),
          COALESCE(b.nama_kampanye,''),
          COALESCE(b.id_produk,''),
          COALESCE(b.jenis_materi_iklan,''),
          COALESCE(b.judul_video,''),
          COALESCE(b.akun_tiktok,''),
          COALESCE(b.status,''),
          COALESCE(b.jenis_otorisasi,''),
          COALESCE(b.mata_uang,''),
          CAST(b.spend AS STRING),
          CAST(b.orders_sku AS STRING),
          CAST(b.cpo AS STRING),
          CAST(b.revenue_gross AS STRING),
          CAST(b.roi AS STRING),
          CAST(b.impressions AS STRING),
          CAST(b.clicks AS STRING),
          CAST(b.ctr AS STRING),
          CAST(b.cvr AS STRING),
          CAST(b.vtr_2s AS STRING),
          CAST(b.vtr_6s AS STRING),
          CAST(b.vtr_25 AS STRING),
          CAST(b.vtr_50 AS STRING),
          CAST(b.vtr_75 AS STRING),
          CAST(b.vtr_100 AS STRING)
        ], '||')
      )) AS row_hash_clean
    FROM base b
  )

  SELECT * FROM with_hash

) S

ON  T.tanggal = S.tanggal
AND T.toko = S.toko
AND T.id_campaign = S.id_campaign
AND T.id_produk = S.id_produk
AND COALESCE(T.id_video,'') = COALESCE(S.id_video,'')

WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    nama_kampanye = S.nama_kampanye,
    jenis_materi_iklan = S.jenis_materi_iklan,
    judul_video = S.judul_video,
    akun_tiktok = S.akun_tiktok,
    status = S.status,
    jenis_otorisasi = S.jenis_otorisasi,
    mata_uang = S.mata_uang,
    spend = S.spend,
    orders_sku = S.orders_sku,
    cpo = S.cpo,
    revenue_gross = S.revenue_gross,
    roi = S.roi,
    impressions = S.impressions,
    clicks = S.clicks,
    ctr = S.ctr,
    cvr = S.cvr,
    vtr_2s = S.vtr_2s,
    vtr_6s = S.vtr_6s,
    vtr_25 = S.vtr_25,
    vtr_50 = S.vtr_50,
    vtr_75 = S.vtr_75,
    vtr_100 = S.vtr_100,
    snapshot_ts = S.snapshot_ts,
    snapshot_date = S.snapshot_date,
    run_id = S.run_id,
    row_hash_raw = S.row_hash_raw,
    row_hash_clean = S.row_hash_clean

WHEN NOT MATCHED THEN
  INSERT ROW;
