@echo off

REM Ganti dengan jalur lengkap ke interpreter Python Anda di virtual environment
set PYTHON_EXE="B:\GitHub Repository\Automated-Crypto-Market-Insights\venv\Scripts\python.exe"

REM Ganti dengan jalur lengkap ke skrip data_collector.py Anda
set SCRIPT_PATH="B:\GitHub Repository\Automated-Crypto-Market-Insights\cleaning\data_cleaning.py"
set ANALYZE_PATH="B:\GitHub Repository\Automated-Crypto-Market-Insights\analysis\analize_data.ipynb"

REM Ganti dengan jalur lengkap ke direktori root proyek Anda
REM Ini penting agar jalur relatif di .env (./database/crypto_data.db) berfungsi dengan benar
set PROJECT_ROOT="B:\GitHub Repository\Automated-Crypto-Market-Insights"

cd /d %PROJECT_ROOT%
%PYTHON_EXE% %SCRIPT_PATH%

%PYTHON_EXE% -m notebook %ANALYZE_PATH%

REM Opsional: Jeda sejenak untuk memastikan log ditulis (hanya jika dijalankan manual untuk debugging)
REM timeout /t 5 /nobreak