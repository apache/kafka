import subprocess

def execute(command):
    if subprocess.run(command).returncode != 0:
        raise SystemError("Failure in executing following command:- ", " ".join(command))

def get_input(message):
    value = input(message)
    if value == "":
        raise ValueError("This field cannot be empty")
    return value