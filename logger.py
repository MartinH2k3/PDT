class Logger:
    def __init__(self, log_path: str = "log.txt"):
        self.log_path = log_path
        # Clear the log file on initialization
        with open(self.log_path, "w") as log_file:
            log_file.write("")

    def info(self, message: str, to_console: bool = True):
        if to_console:
            print(f"[INFO] {message}")
        with open(self.log_path, "a") as log_file:
            log_file.write(f"[INFO] {message}\n")

    def error(self, message: str, to_console: bool = True):
        if to_console:
            print(f"[ERROR] {message}")
        with open(self.log_path, "a") as log_file:
            log_file.write(f"[ERROR] {message}\n")
