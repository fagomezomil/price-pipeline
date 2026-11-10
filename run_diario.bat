@echo off
cd /d D:\Proyectos\precios
set PYTHONIOENCODING=utf-8
"C:\Program Files\Python312\python.exe" src\run_diario.py >> logs\run_diario_last.log 2>&1