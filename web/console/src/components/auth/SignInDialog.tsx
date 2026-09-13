import { useEffect, useState, type FormEvent } from "react";
import { FlaskConical } from "lucide-react";
import { Dialog } from "@/components/ui/overlay";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useConsole } from "@/lib/store";
import { toast } from "@/lib/toast";

export function SignInDialog() {
  const open = useConsole((s) => s.signInOpen);
  const setOpen = useConsole((s) => s.setSignInOpen);
  const mode = useConsole((s) => s.mode);
  const signIn = useConsole((s) => s.signIn);

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (open) {
      setError(null);
      setLoading(false);
    }
  }, [open]);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!/^\S+@\S+\.\S+$/.test(email)) {
      setError("Enter a valid email address.");
      return;
    }
    if (!password) {
      setError("Enter your password.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      await signIn(email, password);
      toast({
        variant: "success",
        title: mode === "demo" ? "Signed in (demo)" : "Signed in",
        description: email,
      });
      setOpen(false);
      setPassword("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sign-in failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={() => setOpen(false)}
      title="Sign in to RAVEN"
      description="Creates a session token for mutating actions (create, cancel, requeue)."
    >
      {mode === "demo" && (
        <div className="mb-4 flex items-start gap-2 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-sm text-warning">
          <FlaskConical className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
          Demo mode — authentication is simulated, any credentials work.
        </div>
      )}
      <form onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4" noValidate>
        <div className="flex flex-col gap-1.5">
          <label htmlFor="signin-email" className="text-sm font-medium text-fg">
            Email
          </label>
          <Input
            id="signin-email"
            type="email"
            autoComplete="email"
            placeholder="ops@raven.dev"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            aria-invalid={error ? true : undefined}
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <label htmlFor="signin-password" className="text-sm font-medium text-fg">
            Password
          </label>
          <Input
            id="signin-password"
            type="password"
            autoComplete="current-password"
            placeholder="••••••••"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            aria-invalid={error ? true : undefined}
          />
        </div>
        {error && (
          <p role="alert" className="text-sm text-error">
            {error}
          </p>
        )}
        <div className="flex justify-end gap-2">
          <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button type="submit" loading={loading}>
            Sign in
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
