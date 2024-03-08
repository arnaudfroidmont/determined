import { useObservable } from 'micro-observables';
import { useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';

import { globalStorage } from 'globalStorage';
import { paths, routeAll } from 'routes/utils';
import { getCurrentUser } from 'services/api';
import { updateDetApi } from 'services/apiConfig';
import authStore, { AUTH_COOKIE_KEY } from 'stores/auth';
import determinedStore from 'stores/determinedInfo';
import { getCookie } from 'utils/browser';
import handleError from 'utils/error';
import { isAuthFailure } from 'utils/service';

const useAuthCheck = (): (() => Promise<boolean>) => {
  const info = useObservable(determinedStore.info);
  const [searchParams] = useSearchParams();

  const updateBearerToken = useCallback((token: string) => {
    globalStorage.authToken = token;
    updateDetApi({ apiKey: `Bearer ${token}` });
  }, []);

  const redirectToExternalSignin = useCallback(() => {
    const { pathname: path, origin, href } = window.location;
    const redirect = [paths.login(), paths.logout()].some((p) => path.includes(p))
      ? origin
      : encodeURIComponent(href);
    const authUrl = `${info.externalLoginUri}?redirect=${redirect}`;
    routeAll(authUrl);
  }, [info.externalLoginUri]);

  const checkAuth = useCallback(async (): Promise<boolean> => {
    /*
     * Check for the auth token from the following sources:
     *   1 - query param jwt from external authentication.
     *   2 - server cookie
     *   3 - local storage
     */
    const jwt = searchParams.getAll('jwt');
    const jwtToken = jwt.length === 1 ? jwt[0] : null;
    const cookieToken = getCookie(AUTH_COOKIE_KEY);
    const authToken = jwtToken ?? cookieToken ?? globalStorage.authToken;

    /*
     * If an auth token is found, validate it with the current user API, then update
     * the API bearer token and set the user as authenticated.
     * If an external login URL is provided, redirect there.
     * Otherwise mark that we checked the auth and skip auth token validation.
     */

    if (authToken) {
      try {
        await getCurrentUser({});
        updateBearerToken(authToken);
        authStore.setAuth({ isAuthenticated: true, token: authToken });
      } catch (e) {
        // If an invalid auth token is detected we need to properly handle the auth error
        handleError(e);
      }
      authStore.setAuthChecked();
    } else if (info.externalLoginUri) {
      try {
        await getCurrentUser({});
      } catch (e) {
        if (isAuthFailure(e)) {
          authStore.setAuth({ isAuthenticated: false });
          redirectToExternalSignin();
          return false;
        }
      }
      authStore.setAuth({ isAuthenticated: true });
      authStore.setAuthChecked();
    } else {
      authStore.setAuthChecked();
    }
    return authStore.isAuthenticated.get();
  }, [info.externalLoginUri, searchParams, redirectToExternalSignin, updateBearerToken]);

  return checkAuth;
};

export default useAuthCheck;
