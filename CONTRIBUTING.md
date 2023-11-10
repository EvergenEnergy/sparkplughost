# Contributing to sparkplughost

Thank you for considering contributing to sparkplughost! Your involvement is crucial to the success of this project.

## Development Workflow

### Branching Strategy

We follow a [trunk-based](https://www.atlassian.com/continuous-delivery/continuous-integration/trunk-based-development) development model, where active development occurs on the `main` branch.

### Code Style

Please ensure that all code contributions adhere to Go's style and best practices as outlined in the [Google Go Style Guide](https://google.github.io/styleguide/go/).

### Linting

We use `golangci` for linting. Before submitting your changes, please run:

```bash
golangci run
```

Address any linting issues before submitting your pull request.

### Testing

Make sure that all unit tests pass by running:

```bash
go test ./...
```

Integration tests are available and can be run by setting the MQTT_BROKER_URL environment variable:

```bash
MQTT_BROKER_URL=tcp://localhost:1883 go test ./...
```

The provided [docker-compose](docker-compose.yml) file contains a [HiveMQ](https://github.com/hivemq/hivemq-community-edition)
service that can be used to run the integration tests against a local broker.

## Commit Messages

We follow the Conventional Commits specification for commit messages. Please format your commit messages accordingly. 
More details can be found at [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

## Pull Requests

Before creating a pull request, please open an issue to discuss the proposed changes. This ensures that the community is aware 
of your intentions, and we can provide guidance or support if needed.

In your pull request description, reference the relevant issue(s) and explain the purpose of your changes. If your pull request 
is related to a specific issue, use the GitHub auto-closing keywords (e.g., "Closes #123") to link the pull request to the corresponding issue.
