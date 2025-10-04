class Logger:
    log_path = "log.txt"
    @staticmethod
    def info(message: str, to_console: bool = True):
        if to_console:
            print(f"[INFO] {message}")
        with open(Logger.log_path, "a") as log_file:
            log_file.write(f"[INFO] {message}\n")

    @staticmethod
    def error(message: str, to_console: bool = True):
        if to_console:
            print(f"[ERROR] {message}")
        with open(Logger.log_path, "a") as log_file:
            log_file.write(f"[ERROR] {message}\n")
