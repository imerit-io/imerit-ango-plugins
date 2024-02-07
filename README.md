## Deployment

### Deploying to Google Cloud Run

#### Prerequisites for GCP

- Enable Cloud Run and Artifact Registry APIs on Google Cloud Platform.
- Configure Workload Identity Federation for GitHub.
- Grant the necessary IAM permissions (Cloud Run Admin, IAM Service Account User, Artifact Registry Admin).
- Check comments from [apprunner-docker.yml](.github%2Fworkflows%2Fapprunner-docker.yml) for more explanation.

#### Steps

1. **GitHub Actions Configuration**

    The `.github/workflows/deploy-gcp-cloud-run.yml` file automates the deployment to Google Cloud Run. Ensure to replace `PROJECT_ID`, `REPOSITORY`, and `REGION` with your GCP project details.

2. **Triggering Deployment**

    The deployment to Cloud Run is triggered by pushing changes to the `main` branch.

### Deploying to AWS App Runner

#### Prerequisites for AWS

- Configure IAM roles for GitHub Actions.
- Create an Amazon ECR repository named `ango-plugins`.
- Check [cloudrun-docker.yml](.github%2Fworkflows%2Fcloudrun-docker.yml) for reference to docs and updating variables
- Refer to following docs for setting up necessary AWS permissions:
- https://aws.amazon.com/blogs/security/use-iam-roles-to-connect-github-actions-to-actions-in-aws/
- https://aws.amazon.com/blogs/containers/deploy-applications-in-aws-app-runner-with-github-actions/

#### Steps

1. **GitHub Actions Configuration**

    The `.github/workflows/deploy-aws-apprunner.yml` file automates the deployment to AWS App Runner. Ensure to replace `AWS_REGION`, `ECR_REPOSITORY`, and `SERVICE` with your AWS details.

2. **Triggering Deployment**

    Similar to GCP, the deployment to AWS App Runner is triggered by pushing changes to the `main` branch or manually via GitHub Actions.
