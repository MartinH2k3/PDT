from utils import build_schema, cleanup_schema, count_all_tables

print(count_all_tables())
with open("history.txt", "a") as f:
    f.write(f"{count_all_tables()}\n")

cleanup_schema()
with open("log.txt", "w") as f:
    f.write("")
build_schema()