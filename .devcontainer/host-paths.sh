# Installed to /etc/profile.d/ — sourced by every login shell in this container.
#
# The Astro stack is launched from inside this dev container, but
# docker-outside-of-docker means its containers are created by Docker Desktop on
# the host, so bind-mount sources are resolved against the HOST filesystem.
# devcontainer.json injects the raw host paths as HOST_WORKSPACE_FOLDER and
# HOST_USER_HOME; Compose needs them in the //c/Users/... form, which is what
# this converts them to.
#
# These must be exported into the environment of the `astro` process itself.
# Putting them in airflow/.env does not work: Astro passes that file to the
# running containers but not to Compose's variable interpolation.

__to_host_path() {
  case "$1" in
    [A-Za-z]:[\\/]*)
      # C:\Users\foo -> //c/Users/foo
      printf '//%s%s' \
        "$(printf '%s' "$1" | cut -c1 | tr '[:upper:]' '[:lower:]')" \
        "$(printf '%s' "${1#?:}" | tr '\\' '/')"
      ;;
    *)
      # Already POSIX (Linux/macOS host) — pass through.
      printf '%s' "$1" | tr '\\' '/'
      ;;
  esac
}

if [ -n "${HOST_WORKSPACE_FOLDER:-}" ]; then
  HOST_PROJECT_ROOT="$(__to_host_path "$HOST_WORKSPACE_FOLDER")"
  export HOST_PROJECT_ROOT
fi

if [ -n "${HOST_USER_HOME:-}" ]; then
  HOST_HOME="$(__to_host_path "$HOST_USER_HOME")"
  export HOST_HOME
fi

unset -f __to_host_path
