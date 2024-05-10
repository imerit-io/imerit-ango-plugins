import subprocess
import re


# test 2
def get_image_id(repository):
    try:
        # Get the output of "sudo docker images"
        result = subprocess.run(
            ["sudo", "docker", "images"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        print("Standard Output:")
        print(result.stdout)

        print("Standard Error:")
        print(result.stderr)

        # Use regular expressions to extract the IMAGE ID for the specified repository
        pattern = re.compile(rf"{repository}\s+\S+\s+(\S+)\s+")
        match = pattern.search(result.stdout)

        if match:
            image_id = match.group(1)
            return image_id
        else:
            print(f"Image with repository '{repository}' not found.")
            return None

    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None


def run_command(command_list):
    try:
        result = subprocess.run(
            command_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        print("Command:", " ".join(command_list))
        print("Standard Output:")
        print(result.stdout)
        print("Standard Error:")
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        print("Standard Output:")
        print(e.stdout)
        print("Standard Error:")
        print(e.stderr)


def run_docker_commands():
    # STOP THE PLUGIN
    run_command(["sudo", "docker", "stop", "export-plugin"])

    # REMOVE THE PLUGIN IMAGE
    run_command(["sudo", "docker", "rm", "-f", "export-plugin"])

    # Get the IMAGE ID for "export-plugin"
    image_id = get_image_id("export-plugin")

    if image_id:
        # FORCE REMOVE THE DOCKER IMAGE
        run_command(["sudo", "docker", "rmi", "-f", image_id])


    # BUILD THE NEW DOCKER IMAGE
    run_command(["sudo", "docker", "build", "-t", "export-plugin:latest", "."])

    # RUN THE DOCKER IMAGE
    run_command(
        [
            "sudo",
            "docker",
            "run",
            "-d",
            "--restart",
            "always",
            "-v",
            "/home/ubuntu/ango-export-plugins/scripts:/app/scripts",
            "--name",
            "export-plugin",
            "export-plugin:latest",
        ]
    )


if __name__ == "__main__":
    run_docker_commands()
