' run_sonda.vbs — lanza la sonda SEPA (un round) oculto, sin parpadear consolas
' Disparado por schtask precios_sonda cada 30 minutos
Dim sh
Set sh = CreateObject("WScript.Shell")
sh.CurrentDirectory = "D:\Proyectos\precios"
sh.Run """C:\Program Files\Python312\python.exe"" src\sonda_ckan.py --once", 0, False
