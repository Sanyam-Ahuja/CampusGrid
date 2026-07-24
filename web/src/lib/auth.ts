import { NextAuthOptions } from "next-auth";
import GoogleProvider from "next-auth/providers/google";

export const authOptions: NextAuthOptions = {
  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_CLIENT_ID || "",
      clientSecret: process.env.GOOGLE_CLIENT_SECRET || "",
    }),
  ],
  callbacks: {
    async signIn({ account, profile }) {
      if (account?.provider === "google" && account.id_token) {
        // Server-side: prefer an internal URL, fall back to the public API URL.
        const apiBase =
          process.env.API_URL ||
          process.env.NEXT_PUBLIC_API_URL ||
          "http://localhost:8000/api/v1";
        try {
          // Exchange Google token for our backend JWT
          const res = await fetch(`${apiBase}/auth/google`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ google_token: account.id_token }),
          });

          if (res.ok) {
            const data = await res.json();
            // Store the backend JWT in the NextAuth account object temporarily
            account.backend_jwt = data.access_token;
            return true;
          }
          console.error(`Backend auth rejected sign-in: ${res.status}`);
        } catch (e) {
          console.error("Failed to authenticate with backend", e);
        }
        // Fail closed: without a backend JWT the app can't make any API calls.
        return false;
      }
      return false;
    },
    async jwt({ token, account }) {
      // Persist the backend JWT to the token right after signin
      if (account?.backend_jwt) {
        token.backend_jwt = account.backend_jwt;
      }
      return token;
    },
    async session({ session, token }) {
      // Send properties to the client
      if (token.backend_jwt) {
        session.backend_jwt = token.backend_jwt as string;
      }
      return session;
    },
  },
  session: { strategy: "jwt" },
};
