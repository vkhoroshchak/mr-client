import random


def generate_input(dest, fd, amount_of_keys, amount_of_values, rows):
    with open(dest, 'w') as input_file:
        for i in range(rows):
            row = ""
            for j in range(amount_of_keys):
                row += chr(random.randint(65, 90))
                row += fd
            for j in range(amount_of_values):
                row += str(random.randint(1, 99))
                row += fd
            row = row[:-1]
            input_file.write(row + '\n')


generate_input("../client_data/input.txt", '|', 2, 1, 100000)
