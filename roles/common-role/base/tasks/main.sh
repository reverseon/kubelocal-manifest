#!/usr/bin/env bash
set -euo pipefail

# Shell translation of: roles/common-role/base/tasks/main.yml
#
# Required inputs (env vars):
#   STAGE                dev1|stg1|prod
#   MF_HOSTNAME          target hostname (e.g. "stg1-ca-redis-cluster-u01")
#   SYS_VAR_BRANCH       git branch/tag for sys_var (e.g. "stg" or "prod")
#   GITHUB_USER          GitHub username (e.g. "MFW-Deploy")
#   GITHUB_ACCESS_TOKEN  GitHub token (password portion for HTTPS auth)
#
# Optional overrides (env vars):
#   MBOOK_BASE           default: /mbook
#   OPT_DOT_KEYS         default: /opt/.keys
#
# Notes:
# - Must be run as root.
# - Will restart+enable SSH service after updating sshd_config.

die() { echo "ERROR: $*" >&2; exit 1; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }

require_root() {
  [[ "${EUID:-$(id -u)}" -eq 0 ]] || die "run as root"
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || die "missing env var: ${name}"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
role_dir="$(cd "${script_dir}/.." && pwd)"     # roles/common-role/base
files_dir="${role_dir}/files"

require_root
need_cmd install
need_cmd chmod
need_cmd chown
need_cmd sed
need_cmd grep
need_cmd git

require_env STAGE
require_env MF_HOSTNAME
require_env SYS_VAR_BRANCH
require_env GITHUB_USER
require_env GITHUB_ACCESS_TOKEN

MBOOK_BASE="${MBOOK_BASE:-/mbook}"
OPT_DOT_KEYS="${OPT_DOT_KEYS:-/opt/.keys}"

MBOOK_MWARE="${MBOOK_BASE}/mware"
MBOOK_LOG="${MBOOK_BASE}/log"
MBOOK_DATA="${MBOOK_BASE}/data"
MBOOK_SYS="${MBOOK_BASE}/sys"
MBOOK_TMP="${MBOOK_BASE}/tmp"
MBOOK_VAR="${MBOOK_SYS}/var"

copy_from_role_files() {
  local src_rel="$1"
  local dest="$2"
  local owner="$3"
  local group="$4"
  local mode="$5"

  local src="${files_dir}/${src_rel}"
  [[ -f "${src}" ]] || die "source file not found: ${src}"
  install -o "${owner}" -g "${group}" -m "${mode}" "${src}" "${dest}"
}

ensure_dir() {
  local path="$1"
  local owner="$2"
  local group="$3"
  local mode="$4"
  install -d -o "${owner}" -g "${group}" -m "${mode}" "${path}"
}

ensure_user_exists() {
  local user="$1"
  id "${user}" >/dev/null 2>&1 || die "required user does not exist: ${user}"
}

restart_sshd() {
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl list-unit-files | grep -qE '^sshd\.service'; then
      systemctl enable --now sshd
      systemctl restart sshd
      return
    fi
    if systemctl list-unit-files | grep -qE '^ssh\.service'; then
      systemctl enable --now ssh
      systemctl restart ssh
      return
    fi
  fi

  if command -v service >/dev/null 2>&1; then
    service sshd restart 2>/dev/null || service ssh restart
    return
  fi

  die "cannot restart ssh service (no systemctl/service)"
}

apt_install_packages() {
  if ! command -v apt-get >/dev/null 2>&1; then
    die "apt-get not found (this script assumes Ubuntu/Debian)"
  fi

  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y \
    build-essential \
    python3-pip \
    python3-setuptools \
    python3 \
    python-is-python3 \
    nmap \
    telnetd \
    tpm-tools-pkcs11 \
    tree \
    mlocate \
    lzop \
    pigz \
    bzip2 \
    bridge-utils \
    rasdaemon \
    dstat \
    sysstat \
    jq \
    unzip \
    ipset \
    postfix \
    git \
    vim \
    rsync \
    rsyslog \
    logrotate
}

git_clone_if_missing() {
  local dest="$1"
  local repo_path="$2" # e.g. moneyforward/sys_var.git
  local ref="$3"
  local become_user="$4"

  if [[ -e "${dest}" ]]; then
    return
  fi

  local url="https://${GITHUB_USER}:${GITHUB_ACCESS_TOKEN}@github.com/${repo_path}"
  # Avoid printing token in logs by keeping output minimal.
  if [[ "${become_user}" == "root" ]]; then
    git clone --depth 1 --branch "${ref}" "${url}" "${dest}"
  else
    ensure_user_exists "${become_user}"
    su -s /bin/bash - "${become_user}" -c \
      "git clone --depth 1 --branch $(printf %q "${ref}") $(printf %q "${url}") $(printf %q "${dest}")"
  fi
}

write_empty_owned_file() {
  local path="$1"
  local owner="$2"
  local group="$3"
  local mode="$4"
  : > "${path}"
  chown "${owner}:${group}" "${path}"
  chmod "${mode}" "${path}"
}

ensure_line_present() {
  local path="$1"
  local owner="$2"
  local group="$3"
  local mode="$4"
  local line="$5"

  if [[ ! -f "${path}" ]]; then
    printf "%s\n" "${line}" > "${path}"
  else
    if ! grep -qxF "${line}" "${path}"; then
      printf "%s\n" "${line}" >> "${path}"
    fi
  fi

  chown "${owner}:${group}" "${path}"
  chmod "${mode}" "${path}"
}

################################################################################
# 1) Copy /etc/resolv.conf (stage-specific)
copy_from_role_files "etc/resolv.conf.${STAGE}" "/etc/resolv.conf" root root 0644

# 2) Install packages
apt_install_packages

# 3) Modify hostname
if command -v hostnamectl >/dev/null 2>&1; then
  hostnamectl set-hostname "${MF_HOSTNAME}"
else
  echo "${MF_HOSTNAME}" > /etc/hostname
  hostname "${MF_HOSTNAME}"
fi

# 4) Modify hostname in /etc/hosts (before it is later replaced by symlink)
if [[ -f /etc/hosts ]] && grep -qE '[[:space:]]ubuntu-initial-installed([[:space:]].*)?$' /etc/hosts; then
  sed -i -E "s/([[:space:]]+)ubuntu-initial-installed([[:space:]].*)?$/\\1${MF_HOSTNAME}\\2/" /etc/hosts
fi

# 5) MOTD
ensure_dir "/etc/motd.d" root root 0755
copy_from_role_files "etc/motd.${STAGE}" "/etc/motd.d/00-env.motd" root root 0644

# 6) sshd_config + restart ssh
copy_from_role_files "etc/ssh/sshd_config" "/etc/ssh/sshd_config" root root 0644
restart_sshd

# 7) Create mbook directories (owned by money-book)
ensure_user_exists "money-book"
ensure_dir "${MBOOK_BASE}" money-book money-book 0755
ensure_dir "${MBOOK_BASE}/app" money-book money-book 0755
ensure_dir "${MBOOK_BASE}/log" money-book money-book 0755
ensure_dir "${MBOOK_MWARE}" money-book money-book 0755
ensure_dir "${MBOOK_MWARE}/config" money-book money-book 0755
ensure_dir "${MBOOK_MWARE}/version" money-book money-book 0755
ensure_dir "${MBOOK_MWARE}/ruby/bin" money-book money-book 0755
ensure_dir "${MBOOK_MWARE}/ssl_cert" money-book money-book 0755
ensure_dir "${MBOOK_LOG}" money-book money-book 0755
ensure_dir "${MBOOK_DATA}" money-book money-book 0755
ensure_dir "${MBOOK_DATA}/run" money-book money-book 0755
ensure_dir "${MBOOK_SYS}" money-book money-book 0755
ensure_dir "${MBOOK_TMP}" money-book money-book 0755
ensure_dir "${OPT_DOT_KEYS}" money-book money-book 0755
ensure_dir "/home/money-book/install_pkg" money-book money-book 0755

# 8) Create empty {{ stage }}_appenv.conf (owned by money-book)
write_empty_owned_file "${OPT_DOT_KEYS}/${STAGE}_appenv.conf" money-book money-book 0644

# 9) Create environment.txt line
ensure_line_present "${MBOOK_BASE}/environment.txt" money-book money-book 0644 "${STAGE}"

# 10) Git clone sys_var if missing
git_clone_if_missing "${MBOOK_VAR}" "moneyforward/sys_var.git" "${SYS_VAR_BRANCH}" "money-book"

# 11) Git clone env if missing
git_clone_if_missing "${MBOOK_TMP}/env" "moneyforward/env.git" "master" "money-book"

# 12) Change /etc/hosts to symbolic link to sys_var (force)
[[ -e "${MBOOK_VAR}/etc/hosts" ]] || die "expected file missing: ${MBOOK_VAR}/etc/hosts"
ln -sfn "${MBOOK_VAR}/etc/hosts" /etc/hosts

