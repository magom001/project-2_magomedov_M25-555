import prompt

LIGHT_BLUE = "\033[96m" # Cyan
RESET = "\033[0m"

HELP_MESSAGE = f"""{LIGHT_BLUE}<command> exit{RESET} - выйти из программы
{LIGHT_BLUE}<command> help{RESET} - справочная информация"""

WELCOME_MESSAGE = f"""Первая попытка запустить проект!

***
{HELP_MESSAGE}"""

COMMAND_PROMPT = "Введите команду: "
UNKNOWN_COMMAND_MESSAGE = "Неизвестная команда: {}"


def welcome():
    """Welcome method that displays the initial interface and handles user commands."""
    print(WELCOME_MESSAGE)
    
    while True:
        user_input = prompt.string(COMMAND_PROMPT)
        
        if user_input == "exit":
            break
        elif user_input == "help":
            print(f"\n{HELP_MESSAGE}")
        else:
            print(UNKNOWN_COMMAND_MESSAGE.format(user_input))