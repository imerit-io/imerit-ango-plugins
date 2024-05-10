# HOW TO START, RESTART, AND STOP AN ANGO PLUGIN DOCKER IMAGE

Ango plugins are running as a docker image, thus, the process for starting, restarting, or stopping a plugin mainly involves working with the docker image.

## PREREQUISITES

You must have the Dockerfile set up in your system. This file contains all the commands a user could call on the command line to assemble your image.
You can copy the example in this repository and modify it to suit your needs.

## STARTING THE ANGO PLUGIN

1. **SSH** into the appropriate EC2 instance.

2. Navigate to the directory containing your Dockerfile:

   ```
   /path/to/your/docker/directory
   ```

   In the case of the export plugin, this would be:

   ```
   /home/shared/ango-export-plugins
   ```

3. **Build the new Docker image**:

   ```
   sudo docker build -t <your-image-name>:<tag> .
   ```

   For instance, if your image is called `export-plugin` and the tag is `latest`, the command would look like:

   ```
   sudo docker build -t export-plugin:latest .
   ```

4. **Run the Docker image**

   ```
   sudo docker run -d --restart always -v /path/to/your/docker/directory/scripts:/app/scripts --name <your-image-name> <your-image-name>:<tag>
   ```

   For the `export-plugin` example, the command would be:

   ```
   sudo docker run -d --restart always -v /home/ubuntu/ango-export-plugins/scripts:/app/scripts --name export-plugin export-plugin:latest
   ```

## RESTARTING THE DOCKER PLUGIN

1. Try the following command to **restart the Docker image**:

   ```
   sudo docker restart <your-image-name>
   ```

   For `export-plugin`, the command would be:

   ```
   sudo docker restart export-plugin
   ```

## STOPPING THE DOCKER PLUGIN

If the restart command fails, you may need to stop the plugin and start it again. Some of this process may seem like overkill, but if there are problems, it's best to follow all these steps to ensure the Docker image is stopped, removed, and then started over.

1. **Stop the plugin**:

   ```
   sudo docker stop <your-image-name>
   ```

   For `export-plugin`, the command would be:

   ```
   sudo docker stop export-plugin
   ```

2. **Remove the plugin image**:

   ```
   sudo docker rm -f <your-image-name>
   ```

   For `export-plugin`, the command would be:

   ```
   sudo docker rm -f export-plugin
   ```

3. **Check Docker images**:

   ```
   sudo docker images
   ```

   This will provide a response similar to this:

   ```
   REPOSITORY      TAG       IMAGE ID       CREATED      SIZE
   <your-image-name>   <tag>    <image-id>   <creation-data>   <size>
   ```

   For example, it might look like this for `export-plugin`:

   ```
   REPOSITORY      TAG       IMAGE ID       CREATED      SIZE
   export-plugin   latest    9dee592ad2af   2 days ago   990MB
   ```

   Take the IMAGE ID for the REPOSITORY you want to remove. In this case, the REPOSITORY we want to remove is `<your-image-name>`, and the IMAGE ID for that Docker image is `<image-id>`.

   NOTE: THESE IMAGE IDs WILL CHANGE SO MAKE SURE YOU CAPTURE THE CORRECT ONE WHEN YOU RUN THE NEXT COMMAND

4. **Force remove the Docker image**:

   ```
   sudo docker rmi -f <IMAGE ID>
   ```

   For instance, using our example from above, you would run:

   ```
   sudo docker rmi -f <image-id>
   ```

5. Now, you can go back to the starting process mentioned above.
