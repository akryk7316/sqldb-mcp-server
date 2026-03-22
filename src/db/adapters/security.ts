export function getBooleanEnv(name: string, defaultValue: boolean): boolean {
  const value = process.env[name]?.trim().toLowerCase();
  if (!value) {
    return defaultValue;
  }

  if (["1", "true", "yes", "on"].includes(value)) {
    return true;
  }

  if (["0", "false", "no", "off"].includes(value)) {
    return false;
  }

  return defaultValue;
}

export function shouldUseEncryptedConnection(defaultValue: boolean): boolean {
  return getBooleanEnv("DB_ENCRYPT", defaultValue);
}

export function shouldFallbackPostgreSQLSSL(err: unknown): boolean {
  const message = err instanceof Error ? err.message : String(err);
  return /server does not support ssl connections/i.test(message);
}
