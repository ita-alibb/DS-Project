import sys
import random
import os

def generate_commands(filename, num_lines=500):
    queues = set()
    commands = []
    
    for _ in range(num_lines):
        if not queues or random.random() < 0.3:  # Create a new queue 30% of the time
            queue_name = f"queue{random.randint(1, 100)}"
            if queue_name not in queues:
                commands.append(f"C {queue_name}")
                queues.add(queue_name)
        elif queues and random.random() < 0.5:  # Add to an existing queue 50% of the time
            queue_name = random.choice(list(queues))
            value = random.randint(1, 999)
            commands.append(f"A {queue_name} {value}")
        else:  # Remove from an existing queue 20% of the time
            queue_name = random.choice(list(queues))
            commands.append(f"R {queue_name}")

    # Save file in the directory where the script is executed
    script_dir = os.getcwd()
    file_path = os.path.join(script_dir, filename)

    with open(file_path, "w") as file:
        file.write("\n".join(commands))

    print(f"File saved as: {file_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    generate_commands(filename)