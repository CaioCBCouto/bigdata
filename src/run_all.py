import subprocess

steps = [
    ["python", "src/ingest/ingest.py"],
    ["python", "src/transform/transform.py"],
]
for s in steps:
    print("\n=== Executando:", " ".join(s))
    subprocess.run(s, check=True)
print("\n=== Concluído!")
