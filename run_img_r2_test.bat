@echo off
cd /d D:\Proyectos\precios
set PYTHONIOENCODING=utf-8
"C:\Program Files\Python312\python.exe" -m pip install pillow --quiet
"C:\Program Files\Python312\python.exe" src\img_r2.py --limit 20
pause
