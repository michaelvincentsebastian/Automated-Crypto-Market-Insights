import React from "react";
import { Search } from "lucide-react";

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  withSearchIcon?: boolean;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className = "", withSearchIcon = false, ...props }, ref) => {
    return (
      <div className="relative flex items-center w-full">
        {withSearchIcon && (
          <Search className="absolute left-3 w-4 h-4 text-slate-500 pointer-events-none" />
        )}
        <input
          ref={ref}
          className={`w-full bg-slate-900/90 border border-slate-800 rounded-lg px-3.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/80 focus:border-indigo-500/80 transition-all ${
            withSearchIcon ? "pl-9" : ""
          } ${className}`}
          {...props}
        />
      </div>
    );
  }
);

Input.displayName = "Input";
