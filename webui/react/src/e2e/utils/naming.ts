/**
 * Generates a naming function
 * @returns The a naming function which uses the same session hash across any imports.
 */
function genSafeName(): { sessionRandomHash: string; safeName: (baseName: string) => string } {
  const sessionRandomHash = randIdAlphanumeric();
  /**
   * Appends a timestamp and a short random hash to a base name to avoid naming collisions.
   * @param baseName The base name to append the timestamp and hash to.
   * @returns The base name with appended timestamp and hash.
   */
  return {
    safeName: (baseName: string): string => {
      const timestamp = Date.now();
      return `${baseName}_${timestamp}_${sessionRandomHash}_${randIdAlphanumeric()}`;
    },
    sessionRandomHash,
  };
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const { sessionRandomHash, safeName } = genSafeName();

/**
 * Generates a four-character random hash
 * @returns Alphanumeric hash
 */
export const randIdAlphanumeric = (): string => Math.random().toString(36).substring(2, 5);

/**
 * Generates a four-character numeric hash
 * @returns Numeric hash
 */
export const randId = (): number => Math.floor(Math.random() * 10_000);
