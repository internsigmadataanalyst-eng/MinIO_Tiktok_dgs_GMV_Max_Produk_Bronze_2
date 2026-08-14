# GMV Max Produk ETL Pipeline — Dokumentasi Data Flow Diagram

**GMV Max Produk ETL Pipeline** — Dokumentasi Data Flow Diagram

Pipeline ETL Iklan TikTok Ads "GMV Max Produk" (Bronze → Silver)

| | | | |
|---|---|---|---|
| **Versi** | 1.1 | **Tanggal** | Agu 2026 |
| **Status** | Draft Internal | **Dibuat oleh** | Tim Data Engineering |

---

## 1. Pendahuluan

Dokumen ini menjelaskan alur data (data flow) pada sistem **GMV Max Produk ETL Pipeline** — sistem otomasi yang mengambil data performa iklan TikTok Ads (GMV Max Produk) dari Google Sheets, membersihkan dan memprosesnya, lalu menyimpannya ke data lake (MinIO) dan data warehouse (DuckDB/BigQuery) dengan pola medallion Bronze → Silver.

Dengan kata sederhana: sistem ini mengambil laporan iklan yang diisi manual di Google Sheets, menggabungkannya, membersihkannya, menyimpannya sebagai arsip, lalu membuat tabel ringkas yang siap dianalisis — semuanya otomatis dan berjalan setiap hari.

**Tujuan dokumen:**
- Memberikan gambaran menyeluruh tentang sumber data, transformasi, dan output sistem
- Mendokumentasikan logika bisnis pada setiap tahap pipeline
- Menjadi referensi teknis untuk pengembangan dan pemeliharaan sistem

### 1.1 Scope sistem

Sistem mencakup 1 pipeline utama yang dijalankan sebagai satu rangkaian berurutan (`python main.py`), terdiri dari 4 tahap:

| Tahap | Nama | Fungsi utama |
|---|---|---|
| 01 | Ingestion GSheet | Mengambil data GMV Max Produk dari 5 Google Sheets dan menggabungkannya menjadi satu DataFrame raw |
| 02 | Bronze Transformation | Membersihkan kolom numerik/tanggal, filter, menambah field snapshot & hash, filter incremental per-sheet |
| 03 | Load MinIO | Menyimpan data bronze sebagai file Parquet di MinIO + memperbarui watermark per-sheet |
| 04 | Silver Merge | Simulasi MERGE Bronze → Silver menggunakan DuckDB in-memory (upsert berdasarkan hash) |

### 1.2 Komponen warna pada diagram

Setiap warna pada diagram memiliki makna yang konsisten:

| Warna | Komponen | Keterangan |
|---|---|---|
| Hijau (teal) | Pipeline | Proses transformasi atau pemrosesan data |
| Ungu | Data store sementara | Hasil antara yang dipakai oleh tahap berikutnya |
| Abu-abu terang | Sumber / output | Data masuk dari luar sistem atau keluar ke end-user |
| Oranye (coral) | Filter / cleansing | Proses penghapusan atau pembersihan data |

---

## 2. Data Flow Diagram — Overview

Diagram berikut menunjukkan gambaran keseluruhan alur data dari 5 Google Sheets sumber hingga output akhir berupa file Parquet di MinIO dan tabel Silver. Setiap tahap menerima input dari tahap sebelumnya dan menghasilkan output yang digunakan oleh tahap selanjutnya.

```
 Google Sheets: "GMV MAX Produk"
   Matz | Ian | Deni | Riwa | Imam
              │
              ▼
        [INGEST]
   Raw DataFrame Concat
   + Tag Sheet Source (creds & sheet_name)
              │
              ▼
      [BRONZE TRANSFORM]
   Clean → Filter → Generate Hash → Apply Watermark
              │
              ▼
        [LOAD MINIO]
      ├── Load Parquet ───────────────► MINIO
      │                                gmv/max/date=…/max_….parquet
      └── Update Watermark ───────────► MINIO
                                       watermarks/gmv_max.json
              │
              ▼
   [LOAD WAREHOUSE: BRONZE]
   Append/Merge → BRONZE_DB.bronze_maxp
              │
              ▼
   [SILVER FORMATTING]
   Pull data kembali dari tabel bronze di warehouse
   Transform memakai sql/silver_merge_tt_ads_gmvmax.sql
              │
              ▼
   [LOAD WAREHOUSE: SILVER]
   Append/Merge → SILVER_DB.silver_tt_ads_gmvmax
```

> Catatan: Pada pipeline ini, langkah "warehouse" (bronze → silver) dijalankan sebagai **simulasi DuckDB in-memory** — tabel di-reset setiap run. Alur yang sama siap dijalankan langsung di BigQuery untuk produksi.

**Apa yang terjadi secara singkat:** Setiap hari, sistem membuka 5 Google Sheets laporan iklan TikTok Ads, menggabungkannya menjadi satu tabel, lalu membersihkan dan menyaring datanya. Data bersih disimpan sebagai arsip Parquet di MinIO, dan dicatat posisi terakhirnya (watermark). Selanjutnya data dimuat ke tabel bronze di warehouse, ditarik kembali dan dibersihkan sekali lagi memakai SQL silver, lalu digabungkan ke tabel silver yang siap dianalisis. Pipeline hanya memproses data yang belum pernah dilihat sebelumnya, sehingga data lama tidak pernah diproses ulang.

### 2.1 Sumber data (inputs)

| Sumber | Tipe | Digunakan oleh |
|---|---|---|
| GSheet "matz" (SH_KEY_MATZ) | Google Sheets | Tahap 01 |
| GSheet "ian" (SH_KEY_IAN) | Google Sheets | Tahap 01 |
| GSheet "deni" (SH_KEY_DENI) | Google Sheets | Tahap 01 |
| GSheet "riwa" (SH_KEY_RIWA) | Google Sheets | Tahap 01 |
| GSheet "imam" (SH_KEY_IMAM) | Google Sheets | Tahap 01 |
| `watermarks/gmv_max.json` | MinIO (JSON) | Tahap 02, 03 |

Kelima Google Sheets memiliki struktur worksheet yang identik (`GMV MAX Produk`), berisi laporan harian performa iklan TikTok Ads per orang (penanggung jawab akun ads).

### 2.2 Output sistem

| Output | Tipe | Dikonsumsi oleh |
|---|---|---|
| `gmv/max/date={YYYYMMDD}/max_{YYYYMMDDHH}.parquet` | MinIO (Parquet) | Tahap 04, konsumen downstream |
| `watermarks/gmv_max.json` (update) | MinIO (JSON) | Tahap 02 (batch berikutnya) |
| `BRONZE_DB.bronze_maxp` | DuckDB in-memory (simulasi) / BigQuery | Tahap 04 (sumber SQL silver) |
| `SILVER_DB.silver_tt_ads_gmvmax` | DuckDB in-memory (simulasi) / BigQuery | Analitik, monitoring, reporting |

> Catatan: Tahap 04 menjalankan simulasi MERGE di DuckDB in-memory (data di-reset setiap run). SQL yang sama siap dijalankan langsung di BigQuery untuk produksi.

---

## 3. Simulasi Alur Data

Bagian ini menelusuri apa yang terjadi pada data ketika `python main.py` dijalankan, menggunakan contoh angka konkret. Tujuannya agar alur di bagian sebelumnya terlihat nyata: dari baris mentah di Google Sheets sampai baris final di tabel Silver.

### 3.1 Asumsi dan konteks run

| Asumsi | Nilai |
|---|---|
| Tanggal eksekusi | 2026-08-13, jam 10 (WIB) |
| Partisi folder (`today_key`) | `20260813` |
| Nama file (`run_key`) | `2026081310` |
| Jumlah sheet | 5 (matz, ian, deni, riwa, imam) |
| Total baris mentah | 200 (50 + 40 + 35 + 45 + 30) |
| Status run | Run pertama (file watermark belum ada) |

### 3.2 Contoh isi sheet mentah

Worksheet `GMV MAX Produk` memiliki 2 baris header di bagian atas dan kolom header di baris ke-3; data dimulai dari baris ke-4. Contoh sebagian isi sheet "matz" (sebagian kolom ditampilkan agar mudah dibaca):

| Tanggal | Toko | ID Campaign | ID Produk | ID Video | Status | Jenis otorisasi | Biaya | Pesanan (SKU) | Pendapatan kotor | ROI | Impresi iklan produk | Jumlah klik | CTR | VTR (2 dtk) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026/08/12 | TOKO SINAR JAYA | 731245678 | 1000123 | 736789012 | Ditayangkan | Business Center | 1.234.567 | 12 | 12.500.000 | 10,12 | 45.000 | 1.234 | 2,74% | 25,5% |
| 2026/08/12 | TOKO LESTARI | 731298745 | 1000456 | 736901234 | Dalam antrean | Otorisasi massal afiliasi | 850.000 | 7 | 8.400.000 | 9,88 | 30.000 | 820 | 2,73% | 20,3% |

Kolom lain pada sheet mencakup Waktu posting, Nama kampanye, Jenis materi iklan, Judul video, Akun TikTok, Mata uang, Biaya per pesanan, Rasio konversi iklan, serta rasio tayang video pada 6, 25, 50, 75, dan 100 detik.

### 3.3 Perjalanan satu baris data

Contoh: baris pertama "TOKO SINAR JAYA" dari sheet "matz".

**Sesudah Tahap 01 — Ingestion:** nilai baris tetap apa adanya, tetapi diberi 2 kolom tambahan: `creds` = nilai env `SH_KEY_MATZ` dan `sheet_name` = `matz`.

**Sesudah Tahap 02 — Bronze Transformation:**

| Kolom bronze | Nilai di sheet (mentah) | Nilai di bronze | Penjelasan |
|---|---|---|---|
| `tanggal` | 2026/08/12 | 2026-08-12 | diubah ke format tanggal baku |
| `biaya` | 1.234.567 | 1234567 (int) | simbol & pemisah ribuan dibuang |
| `pesanan_sku` | 12 | 12 (int) | — |
| `pendapatan_kotor` | 12.500.000 | 12500000 (int) | pemisah ribuan dibuang |
| `impresi_iklan_produk` | 45.000 | 45000 (int) | — |
| `jumlah_klik_iklan_produk` | 1.234 | 1234 (int) | — |
| `roi`, `tingkat_klik_iklan_produk`, `rasio_tayang_video_iklan_2_detik` | 10,12 / 2,74% / 25,5% | tetap teks | angka/persen diolah di Tahap 04 |
| `status`, `jenis_otorisasi` | Ditayangkan / Business Center | sama | lolos whitelist |
| `snapshot_ts`, `snapshot_date` | — | 2026-08-13 03:15 UTC / 2026-08-13 | cap waktu saat diolah |
| `run_id` | — | UUID acak | penanda satu eksekusi |
| `row_hash_raw` | — | SHA-256(tanggal ‖ toko ‖ id_campaign ‖ id_produk ‖ id_video ‖ biaya ‖ impresi_iklan_produk) | sidik jari baris |

**Sesudah Tahap 04 — Silver Merge:**

| Kolom silver | Nilai | Penjelasan |
|---|---|---|
| `toko` | TOKO SINAR JAYA | huruf besar & tanpa spasi tepi |
| `spend` | 1234567 | biaya ÷ scale_factor (default 1) |
| `orders_sku` | 12 | — |
| `revenue_gross` | 12500000 | pendapatan ÷ scale_factor |
| `vtr_2s` | 0.255 | 25,5% → rasio desimal (÷100) |
| `roi`, `ctr`, `cvr`, `vtr_*` | — | teks persen/angka diubah menjadi nilai desimal |
| `row_hash_clean` | SHA-256(...) | sidik jari seluruh dimensi + metrik |

Karena kombinasi (tanggal, toko, id_campaign, id_produk, id_video) ini belum ada di tabel silver, baris di-**INSERT**.

### 3.4 Rekap jumlah baris per tahap (run pertama)

| Tahap | Baris | Keterangan |
|---|---|---|
| Tahap 01 — Ingestion | 200 | 5 sheet digabung (50 + 40 + 35 + 45 + 30) |
| Setelah filter whitelist (status & jenis_otorisasi) | 188 | 12 baris dibuang (nilai tidak dikenal) |
| Setelah buang id_campaign kosong | 183 | 5 baris dibuang |
| Tahap 02 — Bronze (run pertama) | 183 | belum ada watermark → full load |
| Tahap 03 — Parquet di MinIO | 183 | `gmv/max/date=20260813/max_2026081310.parquet` |
| Tahap 04 — Silver | 175 | 8 duplikat kunci dideduplikasi (versi terbaru dipertahankan) |

Verifikasi Tahap 04 membandingkan jumlah baris silver (175) dengan jumlah kunci unik di bronze — hasilnya harus sama, lalu ditampilkan 10 baris sampel.

### 3.5 Skenario eksekusi

| Run | Kondisi watermark | Data yang diproses | File MinIO | Watermark |
|---|---|---|---|---|
| **1 — Pertama kali** | File tidak ada | Full load (183 baris) | `max_2026081310.parquet` | Dibuat: `last_processed_date` = tanggal maksimum per creds (mis. 2026-08-12) |
| **2 — Re-run (mis. jam 11)** | Ada (mis. 2026-08-12) | Hanya baris dengan `tanggal > 2026-08-12` (mis. 12 baris) | `max_2026081311.parquet` | Diperbarui untuk creds yang tanggalnya naik (mis. 2026-08-13) |
| **3 — Tanpa data baru** | Ada (paling baru) | 0 baris | Tidak ada file | Tidak diubah |

Pada skenario 3, pipeline **berhenti** sebelum menulis file karena data sudah up-to-date.

### 3.6 Catatan perilaku data

- **Whitelist**: baris dengan nilai `status` atau `jenis_otorisasi` di luar daftar yang diizinkan dibuang (lihat §5.5).
- **ID kampanye kosong**: baris tanpa ID kampanye selalu dibuang.
- **Tanggal tidak valid**: pada run pertama barisnya tetap diproses (full load), tetapi pada run berikutnya baris tersebut ikut terbuang karena tidak bisa dibandingkan dengan watermark.
- **Watermark gabungan**: bila beberapa sheet berbagi `creds` yang sama, posisi terakhir dihitung dengan nilai MAX agar tidak ada data lama yang diproses ulang.
- **File per jam**: nama file memakai jam (`YYYYMMDDHH`), sehingga re-run di hari yang sama membuat file terpisah tanpa menimpa file lama.
- **Silver bersifat simulasi**: tabel silver hidup di memori DuckDB dan di-reset setiap run. Untuk produksi, SQL yang sama dapat dijalankan langsung di BigQuery.
- **Faktor skala**: tabel `config_gmvmax_scaling` kosong saat ini, sehingga `scale_factor` default = 1 (belum ada aturan penskalaan per toko).

---

## 4. Tahap 01 — Ingestion GSheet

### 4.1 Penjelasan umum

Tahap ini "menyalin" isi laporan dari 5 Google Sheets milik 5 penanggung jawab akun iklan. Setiap sheet dibaca dari worksheet `GMV MAX Produk`, diberi tanda siapa pemiliknya (`creds` dan `sheet_name`), lalu semuanya digabungkan menjadi satu tabel besar. Belum ada pembersihan di sini — data masih apa adanya, termasuk format angka dan tanggal yang berbeda-beda antar baris.

### 4.2 Ringkasan

| | |
|---|---|
| **Tujuan** | Mengambil seluruh data GMV Max Produk dari 5 Google Sheets dan menggabungkannya menjadi satu DataFrame raw |
| **Input** | 5 Google Sheets (worksheet `GMV MAX Produk`) |
| **Output** | DataFrame `gmv_max_produk_raw` (raw, belum dibersihkan) |
| **Trigger** | Setiap kali `run_daily_etl()` dijalankan (pertama kali) |
| **File** | `src/gmv_max_produk/ingestion/fetch_gmv_max_produk_gsheet.py` |

### 4.3 Alur proses

- **Buka spreadsheet**: Iterasi `SHEET_REGISTRY` (`matz`, `ian`, `deni`, `riwa`, `imam`) → buka spreadsheet via key dari env (`SH_KEY_*`) menggunakan service account Google
- **Baca worksheet**: Ambil seluruh nilai worksheet `GMV MAX Produk`
- **Bentuk DataFrame**: Baris mulai dari index 3 (skip 2 baris header), kolom dari index 2; hapus kolom duplikat
- **Tag asal data**: Tambahkan kolom `creds` (key env) dan `sheet_name` (nama orang/akun)
- **Concat**: Gabungkan semua frame per-sheet menjadi satu DataFrame raw (`pd.concat`)

### 4.4 Registrasi sumber sheet

| sheet_name | Key env | Keterangan |
|---|---|---|
| matz | SH_KEY_MATZ | Spreadsheet penanggung jawab 1 |
| ian | SH_KEY_IAN | Spreadsheet penanggung jawab 2 |
| deni | SH_KEY_DENI | Spreadsheet penanggung jawab 3 |
| riwa | SH_KEY_RIWA | Spreadsheet penanggung jawab 4 |
| imam | SH_KEY_IMAM | Spreadsheet penanggung jawab 5 |

> Catatan: Kolom `creds` dan `sheet_name` sengaja **dipertahankan** hingga level bronze untuk mendukung filter incremental per-sheet berdasarkan watermark.

---

## 5. Tahap 02 — Bronze Transformation

### 5.1 Penjelasan umum

Tahap ini adalah "dapur" pipeline: data mentah dibersihkan dan dirapikan. Angka yang tadinya seperti "Rp1.234.567" diubah menjadi angka murni (1234567), tanggal diubah ke format baku, nama kolom dibuat seragam, dan baris yang tidak layak dibuang — misalnya status yang tidak dikenal atau tidak punya ID kampanye. Setiap baris kemudian diberi cap waktu (`snapshot_ts`) dan sidik jari unik (`row_hash_raw`) agar perubahannya bisa dideteksi. Terakhir, hanya data yang benar-benar baru sejak run terakhir yang diteruskan ke tahap berikutnya.

### 5.2 Ringkasan

| | |
|---|---|
| **Tujuan** | Membersihkan data raw, menerapkan filter, menambah field snapshot & hash, dan menyaring data incremental per-sheet berdasarkan watermark |
| **Input** | DataFrame raw (`gmv_max_produk_raw`), `sheet_watermarks` (dari MinIO) |
| **Output** | DataFrame `df_bronze` siap di-load ke `BRONZE_DB.bronze_gmv_max_produk` + `sheet_max_dates` |
| **Trigger** | Setelah Tahap 01 selesai |
| **File** | `src/gmv_max_produk/transform/clean_bronze.py` |

### 5.3 Data flow diagram detail

```
gmv_max_produk_raw
      │
      ▼
[Clean numerik] ───► [Parse tanggal] ───► [snake_case] ───► [Filter key mapping]
                                                                    │
                                                                    ▼
                                                          [Buang id_campaign kosong]
                                                                    │
                                                                    ▼
                                        [Snapshot ts/date + run_id + row_hash_raw]
                                                                    │
                                                                    ▼
                                              [Filter incremental per-sheet (watermark)]
                                                                    │
                                                                    ▼
                                                              df_bronze
```

### 5.4 Alur proses

- **Clean numerik**: `clean_numeric_columns()` pada 6 kolom numerik (`Biaya`, `Pesanan (SKU)`, `Biaya per pesanan`, `Pendapatan kotor`, `Impresi iklan produk`, `Jumlah klik iklan produk`) — bersihkan simbol, normalisasi koma/titik desimal, isi kosong dengan 0, konversi ke integer jika bulat
- **Parse tanggal**: `parse_mixed_dates()` pada kolom `Tanggal` dan `Waktu posting` — mendukung format campuran (YYYY/MM/DD, DD/MM/YYYY, DD/MM/YY, serial Excel, dan ISO)
- **snake_case**: Ubah semua nama kolom ke format snake_case (`to_snake_case()`)
- **Filter key mapping**: `filter_by_key_mapping()` menggunakan `FILTER_CONFIG` (detail di 5.5)
- **Buang baris invalid**: Hapus baris dengan `id_campaign` kosong
- **Field snapshot**: Tambahkan `snapshot_ts` (UTC), `snapshot_date`, dan `run_id` (UUID unik per eksekusi)
- **Hash raw**: Hitung `row_hash_raw` = SHA-256 dari gabungan 7 kolom kunci (`tanggal`, `toko`, `id_campaign`, `id_produk`, `id_video`, `biaya`, `impresi_iklan_produk`)
- **Konversi tipe**: Kolom kuantitas (`pesanan_sku`, `impresi_iklan_produk`, `jumlah_klik_iklan_produk`, `biaya_per_pesanan`, `biaya`) dikonversi ke integer (Int64)
- **Filter incremental**: `filter_by_sheet_watermark()` — per grup `creds`, simpan hanya baris dengan `tanggal` > watermark (detail di 5.6)

### 5.5 Konfigurasi filter (FILTER_CONFIG)

| Kolom | Nilai yang dipertahankan |
|---|---|
| `jenis_otorisasi` | Business Center, Otorisasi massal afiliasi, "", N/A, -, Akun resmi TikTok Shop, Kode video |
| `status` | Ditayangkan, "", Dalam antrean, Perlu otorisasi, Tidak ditayangkan, Tidak tersedia, Mempelajari, Ditolak, Menjelajahi, Tidak aktif, Berperforma buruk, Berperforma baik, Unggulan, Menghitung, Dikecualikan |

Filter bekerja dengan **whitelist** (mempertahankan nilai yang tercantum, membuang sisanya).

### 5.6 Logika watermark incremental

Fungsi `filter_by_sheet_watermark()` pada `utils/minio_client.py`:

| Kondisi | Aksi |
|---|---|
| Grup `creds` **belum punya** watermark | **Full load** — seluruh baris grup dipertahankan |
| Grup `creds` **punya** watermark (tanggal terakhir diproses) | Hanya baris dengan `tanggal` **> watermark** yang dipertahankan |
| Beberapa sheet berbagi `creds` yang sama | Watermark dihitung dengan **MAX** agar tidak ada data lama yang diproses ulang |

Output `sheet_max_dates` berisi tanggal maksimum dari baris yang lolos filter, per grup `creds` — dipakai untuk update watermark di Tahap 03.

---

## 6. Tahap 03 — Load MinIO

### 6.1 Penjelasan umum

Tahap ini "menyimpan arsip". Data bronze diubah menjadi satu file Parquet (format penyimpanan yang ringkas dan cepat dibaca) lalu diunggah ke MinIO — penyimpanan objek yang mirip "gudang file". Setiap run menyimpan file terpisah berdasarkan tanggal dan jam, sehingga tidak ada file yang tertimpa. Tahap ini juga mencatat "posisi terakhir" tiap sheet (watermark) agar run berikutnya hanya mengambil data baru.

### 6.2 Ringkasan

| | |
|---|---|
| **Tujuan** | Menyimpan data bronze sebagai file Parquet di MinIO dan memperbarui watermark per-sheet |
| **Input** | DataFrame `df_bronze`, `watermark_records` (format lama/baru) |
| **Output** | File Parquet `gmv/max/date=.../max_....parquet` + update `watermarks/gmv_max.json` |
| **Trigger** | Setelah Tahap 02 selesai, hanya jika `df_bronze` tidak kosong |
| **File** | `src/gmv_max_produk/pipelines/run_daily_etl.py` (langkah 6–7) |

### 6.3 Alur proses

- **Cek data baru**: Jika `df_bronze` kosong (data sudah up-to-date) → pipeline **berhenti** tanpa menulis file atau watermark
- **Partisi & nama file**:
  - Partisi folder: `gmv/max/date={YYYYMMDD}/` (tanggal eksekusi)
  - Nama file: `max_{YYYYMMDDHH}.parquet` — granularitas jam agar 2 run di hari yang sama menghasilkan file terpisah (tanpa overwrite)
- **Folder marker**: Buat objek penanda folder (0 byte) sebagai partisi
- **Konversi & upload**: `df_bronze.to_parquet(engine="pyarrow")` → upload via `put_object()` ke bucket
- **Update watermark**: `update_sheet_watermarks()` menulis ulang `watermarks/gmv_max.json` dalam format baru

### 6.4 Struktur penyimpanan MinIO

| | |
|---|---|
| Bucket | `tiktok-dgs-gmvmax-produk-bronze` |
| Path file | `gmv/max/date={YYYYMMDD}/max_{YYYYMMDDHH}.parquet` |
| Contoh | `gmv/max/date=20260813/max_2026081310.parquet` |
| Path watermark | `watermarks/gmv_max.json` |
| Format watermark | `{"sheets": [{"creds", "sheet_name", "last_processed_date", "updated_at"}, ...]}` |

### 6.5 Logika update watermark

Fungsi `update_sheet_watermarks()`:

- **Migrasi format (fail-safe)**: Baris format lama (`sheet_name` saja) ditulis ulang menjadi format baru (`creds` + `sheet_name`). Jika gagal, fallback ke `sheet_name` → **full load** (tidak pernah diam-diam menghapus data)
- **Update terpilih**: Hanya sheet yang muncul di `sheet_max_dates` yang mendapatkan `last_processed_date` dan `updated_at` baru; sheet yang sudah up-to-date mempertahankan nilai sebelumnya; sheet baru di-append
- **Tulis**: Payload JSON di-sort per `creds` lalu di-upload kembali ke MinIO

---

## 7. Tahap 04 — Silver Merge

### 7.1 Penjelasan umum

Tahap ini "merapikan rak final". Data bronze yang sudah bersih digabungkan ke tabel silver menggunakan logika MERGE: baris dengan kunci yang sama (tanggal–toko–iklan–produk–video) diperbarui jika isinya berubah, dan baris baru ditambahkan. Setiap baris diberi sidik jari (`row_hash_clean`) sehingga sistem tahu baris mana yang benar-benar berubah. Hasilnya satu baris per kombinasi kunci unik. Di pipeline ini tahapnya dijalankan sebagai simulasi (di dalam memori), tetapi SQL-nya siap dijalankan langsung di BigQuery untuk produksi.

### 7.2 Ringkasan

| | |
|---|---|
| **Tujuan** | Menjalankan simulasi MERGE Bronze → Silver menggunakan DuckDB in-memory (upsert berbasis hash), dengan konfigurasi scaling dari tabel config |
| **Input** | DataFrame `df_bronze`, SQL file `sql/silver_merge_tt_ads_gmvmax.sql` |
| **Output** | Tabel `SILVER_DB.silver_tt_ads_gmvmax` (in-memory) + verifikasi hasil merge |
| **Trigger** | Setelah Tahap 03 selesai (langkah 8 pada `run_daily_etl`) |
| **File** | `src/gmv_max_produk/transform/merge_silver_duckdb.py` + `sql/silver_merge_tt_ads_gmvmax.sql` |

### 7.3 Data flow diagram detail

```
                    df_bronze
                       │
                       ▼
            [LOAD WAREHOUSE: BRONZE]
            Append/Merge → BRONZE_DB.bronze_maxp
                       │
                       ▼
            [PULL BACK dari warehouse bronze]
            Data dibaca ulang dari tabel BRONZE_DB.bronze_maxp
            (join CONFIG_DB.config_gmvmax_scaling utk scale_factor)
                       │
                       ▼
            sql/silver_merge_tt_ads_gmvmax.sql (BigQuery syntax)
                       │
                       ▼
            [_transpile_bq_to_duckdb()]
                       │
                       ▼
            [LOAD WAREHOUSE: SILVER]
            MERGE INTO SILVER_DB.silver_tt_ads_gmvmax
                       │
                       ▼
            [Verifikasi count == distinct(toko,campaign,produk,video,tanggal)]
```

### 7.4 Alur proses

- **Setup schema**: Buat schema `BRONZE_DB`, `SILVER_DB`, `CONFIG_DB` di DuckDB in-memory
- **Load bronze**: `BRONZE_DB.bronze_maxp` diisi dari `df_bronze`
- **Setup config**: `CONFIG_DB.config_gmvmax_scaling(toko, start_date, end_date, scale_factor)` — tabel konfigurasi faktor skala per toko dan periode (saat ini kosong → `scale_factor` default 1)
- **Setup silver**: Buat tabel target `SILVER_DB.silver_tt_ads_gmvmax` (32 kolom, lihat 7.6)
- **Transpile SQL**: `_transpile_bq_to_duckdb()` mengubah sintaks BigQuery menjadi DuckDB (detail 7.5)
- **Eksekusi MERGE**: Jalankan query MERGE (detail 7.7)
- **Verifikasi**: Hitung `COUNT(*)` tabel silver dan bandingkan dengan `COUNT(DISTINCT toko, id_campaign, id_produk, id_video, tanggal)` dari bronze — hasil harus sama (assert), lalu tampilkan 10 baris sampel

### 7.5 Aturan transpilasi BigQuery → DuckDB

| # | Sintaks BigQuery | Menjadi (DuckDB) |
|---|---|---|
| 1 | Prefix `database-sigma.` dan backtick | Dihapus |
| 2 | `MERGE` (tanpa INTO) | `MERGE INTO` |
| 3 | `EXCEPT(...)` | `EXCLUDE (...)` |
| 4 | Raw string `r'...'` | String standard `'...'` |
| 5 | `TO_HEX(SHA256(...))` | `SHA256(...)` (sudah hex) |
| 6 | `ARRAY_TO_STRING` | `array_to_string` |
| 7 | `WHEN NOT MATCHED THEN INSERT ROW` | `INSERT BY NAME` |
| 8 | `SAFE_CAST` | `TRY_CAST` |
| 9 | `INT64` / `FLOAT64` / `NUMERIC` | `BIGINT` / `DOUBLE` / `DECIMAL` |
| 10 | `FORMAT_DATE('%F', ...)` | `STRFTIME(..., '%Y-%m-%d')` |

### 7.6 Skema tabel Silver (`SILVER_DB.silver_tt_ads_gmvmax`)

| Grup | Kolom |
|---|---|
| Dimensi | `tanggal`, `toko`, `nama_kampanye`, `id_campaign`, `id_produk`, `jenis_materi_iklan`, `judul_video`, `id_video`, `akun_tiktok`, `status`, `jenis_otorisasi`, `mata_uang` |
| Metrik | `spend`, `orders_sku`, `cpo`, `revenue_gross`, `roi`, `impressions`, `clicks`, `ctr`, `cvr`, `vtr_2s`, `vtr_6s`, `vtr_25`, `vtr_50`, `vtr_75`, `vtr_100` |
| Teknis | `snapshot_ts`, `snapshot_date`, `run_id`, `row_hash_raw`, `row_hash_clean` |

> **Scaling**: `spend`, `cpo`, `revenue_gross` dibagi `scale_factor` (default 1) dari `config_gmvmax_scaling` berdasarkan `toko` + rentang tanggal.

### 7.7 Logika MERGE (upsert)

Query `silver_merge_tt_ads_gmvmax.sql` bekerja dalam 4 lapisan:

**1. `latest_raw`** — Dedup bronze dengan `ROW_NUMBER()` PARTITION BY `(toko, id_campaign, id_produk, id_video, tanggal)` ORDER BY `snapshot_ts DESC, run_id DESC`, ambil hanya `rn = 1` (versi terbaru).

**2. `scaling_rule`** — Ambil aturan skala dari `config_gmvmax_scaling`; `end_date` kosong dianggap `9999-12-31`.

**3. `base`** — Normalisasi `UPPER(TRIM(...))` pada seluruh kolom dimensi, `SAFE_CAST` metrik, pembagian faktor skala, dan konversi string persen → rasio desimal (/100).

**4. `with_hash`** — Hitung `row_hash_clean` = SHA-256 gabungan (`ARRAY_TO_STRING`) dari seluruh kolom dimensi + metrik.

**Kondisi upsert:**

| Kondisi | Aksi |
|---|---|
| `tanggal, toko, id_campaign, id_produk, id_video` cocok DAN `row_hash_clean` **berbeda** | **UPDATE** seluruh kolom dengan nilai baru (data berubah) |
| `tanggal, toko, id_campaign, id_produk, id_video` cocok DAN hash **sama** | Tidak ada aksi (data identik) |
| Kunci **tidak ditemukan** di target | **INSERT** baris baru (`INSERT BY NAME`) |

---

## 8. Katalog Data Store

### 8.1 Google Sheets

| Nama logis | Worksheet | Dibaca oleh | Keterangan |
|---|---|---|---|
| source_matz | GMV MAX Produk | Tahap 01 | Laporan iklan per penanggung jawab |
| source_ian | GMV MAX Produk | Tahap 01 | Laporan iklan per penanggung jawab |
| source_deni | GMV MAX Produk | Tahap 01 | Laporan iklan per penanggung jawab |
| source_riwa | GMV MAX Produk | Tahap 01 | Laporan iklan per penanggung jawab |
| source_imam | GMV MAX Produk | Tahap 01 | Laporan iklan per penanggung jawab |

### 8.2 MinIO

| | |
|---|---|
| Bucket | `tiktok-dgs-gmvmax-produk-bronze` |
| Data bronze | `gmv/max/date={YYYYMMDD}/max_{YYYYMMDDHH}.parquet` |
| Watermark | `watermarks/gmv_max.json` |
| Isi | File Parquet hasil Tahap 03 + state watermark incremental per-sheet |
| Konsumen | Tahap 04 (simulasi silver), pipeline/sistem downstream |

### 8.3 DuckDB / BigQuery (Silver)

| Tabel | Diisi oleh | Keterangan |
|---|---|---|
| `BRONZE_DB.bronze_maxp` | Tahap 02/03 | Staging bronze (in-memory saat simulasi) |
| `SILVER_DB.silver_tt_ads_gmvmax` | Tahap 04 | Hasil MERGE bronze → silver, 1 baris per kunci unik |
| `CONFIG_DB.config_gmvmax_scaling` | Manual / sumber lain | Konfigurasi `scale_factor` per toko & periode (saat ini kosong) |

---

## 9. Jadwal Eksekusi Harian

Pipeline dijalankan sebagai satu rangkaian tunggal (`python main.py` → `run_daily_etl()`), dengan 4 tahap yang memiliki dependensi linear:

| Urutan | Tahap | Dependensi | Keterangan |
|---|---|---|---|
| 1 | 01 — Ingestion GSheet | — | Membaca 5 Google Sheets |
| 2 | 02 — Bronze Transformation | Tahap 01 selesai | Cleaning, filter, hash, watermark filter |
| 3 | 03 — Load MinIO | Tahap 02 selesai | Upload parquet + update watermark |
| 4 | 04 — Silver Merge | Tahap 03 selesai | Simulasi MERGE bronze → silver |

**Alur keputusan penting:**

- Jika hasil filter watermark Tahap 02 **kosong** (tidak ada data baru) → pipeline berhenti di awal Tahap 03 tanpa menulis file
- Granularitas jam pada nama file (`YYYYMMDDHH`) memungkinkan re-run di hari yang sama menghasilkan file terpisah tanpa overwrite
- Tahap 04 bersifat simulasi verifikasi (DuckDB in-memory); untuk produksi, SQL yang sama dapat dijalankan langsung di BigQuery

**Cara menjalankan pipeline dari root project:**
```
python main.py
```

---

## 10. Changelog

| Versi | Tanggal | Perubahan |
|---|---|---|
| 1.2 | Agu 2026 | Perbarui diagram alur — tambah tahap warehouse bronze → silver (pull data kembali dari `BRONZE_DB.bronze_maxp`, transform memakai SQL silver, load ke `SILVER_DB.silver_tt_ads_gmvmax`) |
| 1.1 | Agu 2026 | Tambah bagian Simulasi Alur Data (contoh angka), penjelasan umum per tahap, koreksi jumlah kolom `row_hash_raw` (7 kolom) & skema silver (32 kolom), klarifikasi status simulasi silver dan tabel config kosong |
| 1.0 | Jan 2026 | Dokumentasi DFD pertama — pipeline ETL GMV Max Produk (Ingest → Bronze → MinIO → Silver) |

**Konfidensial — Internal**
