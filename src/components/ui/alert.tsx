import React from "react";
import { AlertTriangle, Info, CheckCircle2, XCircle } from "lucide-react";

interface AlertProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: "warning" | "info" | "success" | "danger";
  title?: string;
  description?: string;
  action?: React.ReactNode;
}

export function Alert({
  variant = "warning",
  title,
  description,
  action,
  className = "",
  children,
  ...props
}: AlertProps) {
  const styles = {
    warning: {
      bg: "bg-amber-500/10 border-amber-500/30 text-amber-300",
      icon: <AlertTriangle className="w-5 h-5 text-amber-400 shrink-0" />,
    },
    info: {
      bg: "bg-indigo-500/10 border-indigo-500/30 text-indigo-300",
      icon: <Info className="w-5 h-5 text-indigo-400 shrink-0" />,
    },
    success: {
      bg: "bg-emerald-500/10 border-emerald-500/30 text-emerald-300",
      icon: <CheckCircle2 className="w-5 h-5 text-emerald-400 shrink-0" />,
    },
    danger: {
      bg: "bg-rose-500/10 border-rose-500/30 text-rose-300",
      icon: <XCircle className="w-5 h-5 text-rose-400 shrink-0" />,
    },
  };

  const current = styles[variant];

  return (
    <div
      role="alert"
      className={`flex items-start gap-3 p-4 rounded-xl border ${current.bg} ${className}`}
      {...props}
    >
      {current.icon}
      <div className="flex-1">
        {title && <h4 className="font-semibold text-sm leading-tight">{title}</h4>}
        {description && <p className="text-xs text-slate-300 mt-0.5 leading-relaxed">{description}</p>}
        {children}
      </div>
      {action && <div className="shrink-0">{action}</div>}
    </div>
  );
}
