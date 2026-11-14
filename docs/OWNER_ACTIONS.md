# Owner Actions — senin yapacakların (adım adım)

> **Document Version**: 1.2 (2026-11-14, M2 release adımları) — Türkçe, komut listesi biçiminde (sahibin isteği)
> **Nerede çalıştırılır**: Windows **PowerShell**, depo kök klasöründe:
> `cd C:\Users\admin\Desktop\WS-Projects\thelook-lakehouse-platform\thelook-lakehouse-platform`
> **Dal değişince bu dosya kaybolursa**: GitHub'daki kopyası →
> [OWNER_ACTIONS.md (feat/w5-aws-foundation)](https://github.com/dogruom/thelook-lakehouse-platform/blob/feat/w5-aws-foundation/docs/OWNER_ACTIONS.md)

**Kurallar**
- Komut bloklarını **sırayla** çalıştır; her bloktan sonra "Görmen gereken" ile karşılaştır.
- "Bana gönder" satırındaki çıktıyı sohbete yapıştır. **Asla** `.env` içeriği, şifre, anahtar,
  `terraform.tfstate` yapıştırma (hesap numarası ve ARN gizli değildir, onlar sorun değil).
- Bir komut hata verirse dur, hatanın son 20 satırını bana gönder.

**Özet sıra**

| # | Ne | Süre | Ne zaman |
|---|---|---|---|
| 1 | Docker'a daha fazla bellek ver, yeniden başlat | 5 dk | **önce bu** |
| 2 | `.env`'e eksik anahtarları ekle, Airflow'u yeniden başlat | 5 dk | 1'den hemen sonra |
| 3 | ~~Bana "Docker hazır" yaz~~ ✅ yapıldı (backfill 1 passed) | — | — |
| 4 | Uçtan uca pipeline'ı `land` dahil koş (**M2 için zorunlu**) | 30 dk | **şimdi** (stack açık) |
| 4b | Öğrenme kapıları W1–W4 (**M2 için zorunlu**) | 4 × 20 dk | istediğin zaman, 5'ten önce |
| 5 | `v0.2.0` etiketi (**M2'nin son adımı**) | 2 dk | ben "M2 hazır, etiketle" dediğimde |
| 6 | AWS 1. gün: hesap + araçlar + bootstrap | 45 dk | istediğin zaman |
| 7 | AWS bulut günü: kur → koş → doğrula → **sil** | 1,5 saat | 6'dan sonra |
| 8 | GitHub `aws-dev` ortamı | 10 dk | 7'yi bir kez yaptıktan sonra |
| 9 | Databricks Free Edition hesabı | 10 dk | W6'dan önce |

---

## Adım 1 — Docker belleği ve yeniden başlatma

**Neden**: Docker bugün 4 kez çöktü; 7,6 GB bellek, Spark + Airflow + Thrift için dar.

```powershell
# 1a) Bilgisayarındaki toplam RAM (GB):
[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB)

# 1b) WSL ayar dosyasını aç (yoksa Not Defteri "oluşturulsun mu?" diye sorar → Evet):
notepad "$env:USERPROFILE\.wslconfig"
```
Dosyaya şunu yaz ve kaydet (`memory` = RAM'inin yarısını geçmesin; 16 GB RAM → `8GB`, 32 GB → `12GB`):
```ini
[wsl2]
memory=10GB
swap=4GB
```
```powershell
# 1c) WSL'i kapat (Docker Desktop da kapanır):
wsl --shutdown
# 1d) Docker Desktop'ı Başlat menüsünden aç, sol altta "Engine running" yazana kadar bekle (~1 dk).
# 1e) Kontrol:
docker info --format "{{.MemTotal}}"
```
**Görmen gereken**: 1e'de 7,6 GB'tan büyük bir sayı (bayt cinsinden, ör. `10450000000`).
**Bana gönder**: 1e'nin çıktısı.

## Adım 2 — Eksik `.env` anahtarları + Airflow'u yeniden başlat

**Neden**: Airflow'un JWT anahtarı (`AIRFLOW__API_AUTH__JWT_SECRET`) `.env`'de yok. Komut eksik
anahtarları rastgele değerle ekler, hiçbir değeri ekrana yazmaz, mevcut değerleri değiştirmez.

```powershell
# 2a) Airflow servislerinin tanımı bu dalda:
git switch feat/w4-d22-airflow

# 2b) Eksik anahtarları ekle ve kontrol et:
.\.venv\Scripts\Activate.ps1
python scripts/bootstrap_env.py --add-missing
python scripts/check_env.py

# 2c) Stack'i başlat (Airflow, yeni anahtarla yeniden oluşturulur):
docker compose --profile core --profile sql up -d
docker compose --profile core --profile sql --profile orchestration up -d --force-recreate airflow-scheduler
docker compose ps --format "table {{.Name}}\t{{.Status}}"
```
**Görmen gereken**: 2b'de `all keys set`; 2c'de `crm_*` konteynerleri `Up ... (healthy)`.
**Bana gönder**: 2b'nin son satırı + 2c'nin tablosu. **Dalı değiştirme**, `feat/w4-d22-airflow`'da kalsın.

## Adım 3 — Bana haber ver

Sohbete **"Docker hazır"** yaz. Ben backfill testini (W4.D23.4, ~55 dk) koşar, sonucu yazar ve PR #21'i
koşullar sağlanırsa merge ederim. Bu sırada Docker'a dokunma (bilgisayarı kullanabilirsin).

## Adım 4 — Uçtan uca pipeline (`land` dahil)

**Ne zaman**: şimdi yapılabilir (backfill ve DAG testleri bitti). **Neden**: `land` adımı yerel S3 (SeaweedFS) şifresini
istiyor; ben `.env` okuyamam, bu yüzden senin terminalinde koşmalı.

```powershell
# 4a) M2'nin son dalı (PR #19):
git switch feat/w4-d24-e2e-contracts
.\.venv\Scripts\Activate.ps1
docker compose --profile core --profile sql up -d

# 4b) Sadece bu terminale S3 anahtarlarını yükle (ekrana bir şey yazmaz):
Get-Content .env | Where-Object { $_ -match '^(S3_ACCESS_KEY|S3_SECRET_KEY)=' } |
  ForEach-Object { $k, $v = $_ -split '=', 2; Set-Item "env:$k" $v }
$env:S3_ENDPOINT = "http://127.0.0.1:9000"

# 4c) Koş (~30 dk):
python scripts/run_e2e.py

# 4d) Anahtarları terminalden sil:
Remove-Item env:S3_ACCESS_KEY, env:S3_SECRET_KEY, env:S3_ENDPOINT
```
**Görmen gereken**: en sonda her adım (`produce`, `land`, `bronze_stream`, … `dbt_build`) için `ok`.
**Bana gönder**: 4c'nin son ~20 satırı (özet tablo).

## Adım 4b — Öğrenme kapıları (M2 için)

**Neden**: M2 kapanışı (checkpoint) bir öğrenme kapısıyla biter; W1–W2 kapısı 2026-11-11'den beri
yanıt bekliyor (V-11), W3 ve W4 hiç koşulmadı. Bu adım bir sohbet; terminal komutu yok.

Her biri için ayrı bir oturumda (veya bu sohbette) sırayla yaz:
```text
beni test et W1
beni test et W2
beni test et W3
beni test et W4
```
Her kapı birkaç soru sorar, cevaplarına göre eksikleri anlatır ve tekrar sorar; sonunda
`.claude/learning/W<n>_REHBER.md` çalışma rehberi ve görsel özetler üretilir.
**Bana gönder**: gerek yok — kapı sonuçları dosyaya yazılır.

**Bağımsız güvenlik incelemesi (V-19, M1'den beri açık)** — PR #21 ve #19 `main`'e merge edildikten
sonra, Claude Code sohbetinde `main` dalındayken şunu yaz (bir slash komutu, terminal değil):
```text
/security-review
```
**Bana gönder**: bulduğu HIGH/MEDIUM maddeler (yoksa "temiz").

## Adım 5 — `v0.2.0` etiketi

**Ne zaman**: sadece ben "M2 hazır, etiketle" dediğimde (reviewer APPROVE, auditor PASS, PR #21 ve #19
`main`'e merge edildi, Adım 4 ve 4b tamam).

```powershell
git switch main
git pull --ff-only
git log --oneline -3
git tag -a v0.2.0 -m "M2: data correctness and analytics products"
git push origin v0.2.0
```
**Görmen gereken**: [Tags sayfasında](https://github.com/dogruom/thelook-lakehouse-platform/tags) `v0.2.0`.
**Bana gönder**: "tag atıldı".

---

## Adım 6 — AWS 1. gün: hesap, araçlar, bootstrap (bir kez)

**6a) Tarayıcıda (konsol)**
1. [Kredi / Free plan](https://console.aws.amazon.com/billing/home#/credits): USD 100 kredi ve plan bitiş tarihini gör.
2. [Security credentials](https://console.aws.amazon.com/iam/home#/security_credentials) → **Assign MFA** (root için). Root access key **oluşturma**.

**6b) Araçları kur (bir kez)**
```powershell
winget install --id Amazon.AWSCLI -e
winget install --id Hashicorp.Terraform -e
```
PowerShell'i **kapatıp yeniden aç**, depo klasörüne geri dön, sonra:
```powershell
aws --version          # 2.32 veya üstü olmalı
terraform version      # 1.10 veya üstü olmalı
```

**6c) AWS'ye giriş (anahtar dosyası yok, tarayıcıdan onay)**
```powershell
aws login --profile crm --region eu-central-1
$env:AWS_PROFILE = "crm"
aws sts get-caller-identity
```
**Bana gönder**: `get-caller-identity` çıktısı (Account + Arn).
`aws login` "unknown command" derse dur ve bana yaz — alternatif yol [runbook §1](runbooks/aws-owner-setup.md)'de.

**6d) Bootstrap: Terraform state bucket'ı + bütçe alarmları**
```powershell
git switch feat/w5-aws-foundation
$env:TF_VAR_budget_email = "<e-posta-adresin>"     # bütçe alarmlarının gideceği adres
terraform -chdir=infra/terraform/bootstrap init
terraform -chdir=infra/terraform/bootstrap plan -out bootstrap.plan
```
**Görmen gereken**: `Plan: N to add, 0 to change, 0 to destroy` (1 bucket + ayarları, 1 budget).
**Bana gönder**: o `Plan:` satırı. Ben "tamam" deyince:
```powershell
terraform -chdir=infra/terraform/bootstrap apply bootstrap.plan
terraform -chdir=infra/terraform/bootstrap output
```
Sonra e-postana gelen **AWS budget aboneliğini onayla** ve [Budgets](https://console.aws.amazon.com/billing/home#/budgets)
sayfasında 5 / 20 / 40 USD alarmlarını gör.
**Bana gönder**: `output` çıktısı (bucket adı). `infra/terraform/bootstrap/terraform.tfstate` dosyasını **silme, paylaşma**.

## Adım 7 — AWS bulut günü: kur → koş → doğrula → sil

Her bulut gününde aynı akış. Önce giriş:
```powershell
git switch feat/w5-aws-foundation
.\.venv\Scripts\Activate.ps1
aws login --profile crm --region eu-central-1
$env:AWS_PROFILE = "crm"
```

**7a) Kaynakları planla** (ilk seferde backend dosyası oluşur):
```powershell
$state = terraform -chdir=infra/terraform/bootstrap output -raw state_bucket
"bucket = `"$state`"" | Set-Content -Encoding utf8 infra/terraform/envs/dev/backend.hcl
terraform -chdir=infra/terraform/envs/dev init -backend-config=backend.hcl
$env:TF_VAR_owner_principal_arn = (aws sts get-caller-identity --query Arn --output text)
terraform -chdir=infra/terraform/envs/dev plan -no-color -out dev.plan > dev-plan.txt
Get-Content dev-plan.txt -Tail 5
```
**Bana gönder**: `dev-plan.txt` dosyasının tamamı (gizli bilgi içermez). **Ben onaylamadan apply etme.**

**7b) Onaydan sonra kur**:
```powershell
terraform -chdir=infra/terraform/envs/dev apply dev.plan
$env:LAKEHOUSE_AWS_BUCKET = (terraform -chdir=infra/terraform/envs/dev output -raw data_bucket)
```
"already exists" hatası alırsan çıktıyı bana gönder (GitHub OIDC provider / Glue log grupları zaten varsa iki ayar gerekiyor).

**7c) Kodu ve veriyi yükle, Glue işlerini koş**:
```powershell
python scripts/cloud/build_artifacts.py
python -m lakehouse.generator land --target aws
docker compose --profile core up -d
python scripts/cloud/seed_bronze.py                 # önce kuru deneme: ne kopyalanacak
python scripts/cloud/seed_bronze.py --apply
python scripts/cloud/run_job.py register_tables --record
python scripts/cloud/run_job.py --sequence cloud --record
```
İzle: [Glue jobs](https://eu-central-1.console.aws.amazon.com/gluestudio/home?region=eu-central-1#/jobs).
**Bana gönder**: son iki komutun çıktısı (iş adı, SUCCEEDED/FAILED, DPU-saniye).
`seed_bronze.py` S3 anahtarı isterse önce Adım 4b'yi bu terminalde çalıştır.

**7d) Kiracı (tenant) izolasyonu kanıtı** — `brand_alpha` rolü `brand_beta` klasörünü okuyamamalı:
```powershell
$alpha = terraform -chdir=infra/terraform/envs/dev output -raw tenant_brand_alpha_role_arn
$c = aws sts assume-role --role-arn $alpha --role-session-name tenant-demo --query Credentials --output json | ConvertFrom-Json
$env:AWS_ACCESS_KEY_ID = $c.AccessKeyId; $env:AWS_SECRET_ACCESS_KEY = $c.SecretAccessKey; $env:AWS_SESSION_TOKEN = $c.SessionToken
aws s3 ls "s3://$env:LAKEHOUSE_AWS_BUCKET/landing/brand_alpha/"     # liste gelmeli
aws s3 ls "s3://$env:LAKEHOUSE_AWS_BUCKET/landing/brand_beta/"      # AccessDenied gelmeli
Remove-Item env:AWS_ACCESS_KEY_ID, env:AWS_SECRET_ACCESS_KEY, env:AWS_SESSION_TOKEN
```
**Bana gönder**: iki `ls` komutunun çıktısı (gizli bilgi içermez; `$c`'yi yazdırma).

**7e) Gold katmanı Athena'da** (ilk seferde sanal ortam kurulur):
```powershell
py -3.11 -m venv .venv-dbt-athena
.\.venv-dbt-athena\Scripts\python -m pip install -r requirements/dbt-athena.txt
$env:DBT_ATHENA_S3_STAGING_DIR = "s3://$env:LAKEHOUSE_AWS_BUCKET/athena-results/"
$env:DBT_ATHENA_S3_DATA_DIR = "s3://$env:LAKEHOUSE_AWS_BUCKET/gold/"
cd dbt_project
..\.venv-dbt-athena\Scripts\dbt run-operation register_athena_silver --target athena --profiles-dir profiles --vars "{athena_silver_root: 's3://$env:LAKEHOUSE_AWS_BUCKET/silver'}"
..\.venv-dbt-athena\Scripts\dbt build --target athena --select staging marts --profiles-dir profiles
cd ..
```
**Bana gönder**: `dbt build`'in son satırı (`PASS=… WARN=… ERROR=…`).
İstersen sorguları [Athena konsolunda](https://eu-central-1.console.aws.amazon.com/athena/home?region=eu-central-1#/query-editor) (workgroup `crm-lakehouse-dev`) da görebilirsin.

**7f) SİL — bunu asla atlama** (para harcamasını durdurur):
```powershell
terraform -chdir=infra/terraform/envs/dev destroy
aws glue get-jobs --query "Jobs[].Name"          # [] olmalı
aws s3 ls | Select-String crm-lakehouse          # sadece ...-tfstate-... kalmalı
```
Ertesi gün harcamayı [Bills](https://console.aws.amazon.com/billing/home#/bills) veya
[Cost Explorer](https://console.aws.amazon.com/costmanagement/home#/cost-explorer)'da gör (24 saat gecikebilir).
**Bana gönder**: iki kontrol çıktısı + günün harcaması (USD, kredi öncesi ve sonrası).

## Adım 8 — GitHub `aws-dev` ortamı (bir kez, 7b'den sonra bir bulut gününde)

1. [Settings → Environments](https://github.com/dogruom/thelook-lakehouse-platform/settings/environments) → **New environment** → adı `aws-dev` → *Required reviewers*: kendin → *Deployment branches*: sadece `main`.
2. [Settings → Actions → Variables](https://github.com/dogruom/thelook-lakehouse-platform/settings/variables/actions) → **Variables** sekmesi (Secrets değil) → üç değişken:
   ```powershell
   # değerleri almak için:
   terraform -chdir=infra/terraform/envs/dev output -raw github_deploy_role_arn
   terraform -chdir=infra/terraform/envs/dev output -raw data_bucket
   ```
   `AWS_REGION` = `eu-central-1` · `AWS_DEPLOY_ROLE_ARN` = ilk çıktı · `LAKEHOUSE_AWS_BUCKET` = ikinci çıktı
3. [Actions](https://github.com/dogruom/thelook-lakehouse-platform/actions) → `deploy-and-run` → **Run workflow** → onay isteğini **Approve**.
**Bana gönder**: workflow çalışmasının linki. Sonra **7f (sil)**.

## Adım 9 — Databricks Free Edition (W6'dan önce, bir kez)

1. [Databricks Free Edition](https://www.databricks.com/learn/free-edition) → kayıt ol (kredi kartı yok, ücretli plana geçme).
2. CLI ve giriş (tarayıcıdan onay, token dosyası yok):
```powershell
winget install Databricks.DatabricksCLI
# PowerShell'i kapatıp aç, sonra (adres = tarayıcıdaki workspace adresin):
databricks auth login --host https://<workspace-adresin>.cloud.databricks.com
databricks current-user me
```
**Bana gönder**: workspace adresi + `current-user me` çıktısındaki `userName`.

## Ek — Öğrenme kapıları (istediğin zaman)

Sohbette **"beni test et W4"** veya **"beni test et W5"** yaz.
