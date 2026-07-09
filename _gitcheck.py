import subprocess, os
os.chdir(r'c:\Live_MigrationProject\Databrciks_Poc\Poc\MigrationtoDBXAssetBundle')
subprocess.run(['git', 'add', '-A'])
r = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
r2 = subprocess.run(['git', 'log', '--oneline', '-3'], capture_output=True, text=True)
out = f"STATUS:\n{r.stdout}\nLOG:\n{r2.stdout}"
with open(r'c:\Live_MigrationProject\Databrciks_Poc\Poc\MigrationtoDBXAssetBundle\gitstate.txt', 'w') as f:
    f.write(out)
print(out)
