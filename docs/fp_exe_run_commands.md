# Running `fp.exe` (Fair–Parke / FP program) from the command line
This is a practical command reference for **running the Fair–Parke (FP) program executable (`fp.exe`)** in a reproducible way.

It’s written to be “LLM-friendly”: copy/pasteable commands + minimal narrative.

> Notes
> - Official docs often show the executable as `FP` at a DOS prompt. On Windows, that’s effectively `fp.exe`.
> - FP is designed around **command input files** (e.g., `FMINPUT.TXT`, `JOB.INP`) and a **piped output file** (often `OUT`).

---

## 0) Local environment (this repo + macOS Wine)
For this project on this machine, use:
- Wine: `/opt/homebrew/bin/wine`
- Model root: `/path/to/your/FM`

```bash
# Optional: set once for this shell
export FP_HOME=/path/to/your/FM
export WINE_BINARY=/opt/homebrew/bin/wine
```

Quick smoke test from shell:
```bash
cd "$FP_HOME"
printf "INPUT FILE=fminput.txt;\n" > /tmp/FPINPUT.in
"$WINE_BINARY" fp.exe > /tmp/fp_sanity.out < /tmp/FPINPUT.in
```

Project CLI entrypoint (runs scenarios through `fp-wraptr`):
```bash
cd /path/to/fp-wraptr
FP_HOME="$FP_HOME" uv run fp run examples/baseline.yaml
```
(or `--fp-home "$FP_HOME"` explicitly on the command).

---

## 0) Mental model: how FP runs
FP is a command-line program that:
1) Starts up and asks for an `INPUT` command (or you supply it via stdin).
2) Runs commands from a **job input file** (default name: `JOB.INP`).
3) Stops when it reaches **`QUIT;` / `EXIT;`** (end job) or **`KEYBOARD;` / `RETURN;`** (drop to interactive mode).

### Key FP commands you’ll use for automation
- `INPUT FILE=SomeFile.INP;` — start executing commands from a file
- `QUIT;` or `EXIT;` — terminate the job
- `KEYBOARD;` or `RETURN;` — return control to the keyboard (interactive mode)
- `HELP;` — list FP commands (useful for discovery)
- Format: **commands end with a semicolon** `;` (and FP keeps reading until it sees one).

---

## 1) Recommended “batch mode” pattern (fast + repeatable)
The FP User’s Guide recommends:
- Put all commands in an input file (e.g., `IS.INP`, `FMINPUT.TXT`).
- Run FP and **pipe output to a file** (example name `OUT`) so you can inspect/search it later.
- Optionally, fully automate keyboard typing with stdin redirection.

You’ll see this pattern everywhere:

### Pattern A: manual batch-mode run (type the INPUT line after launching)
**Windows (cmd.exe):**
```bat
cd fm
fp.exe > OUT
REM wait a couple seconds for fp.exe to load...
INPUT FILE=FMINPUT.TXT;
```

⚠️ Important: after you start `fp.exe > OUT`, you may **not** get prompted to type the `INPUT FILE=...;` line—just begin typing when the program is ready.

### Pattern B: fully non-interactive batch-mode run (best for scripts/Python)
This avoids the “wait a couple seconds then type” step by putting the `INPUT ...;` command into a small file and redirecting stdin.

**Windows (cmd.exe):**
```bat
cd fm
echo INPUT FILE=FMINPUT.TXT;> IN
fp.exe > OUT < IN
```

**Linux/macOS (shell):**
```bash
cd fm
printf "INPUT FILE=FMINPUT.TXT;\n" > IN
"/opt/homebrew/bin/wine" fp.exe > OUT < IN
```

### Pattern C: old-school DOS automation with a .BAT (as documented)
Create `FPRUN.BAT` containing:
```bat
fp.exe > OUT < IN
```

Create `IN` containing:
```text
INPUT FILE=FMINPUT.TXT;
```

Then run:
```bat
FPRUN
```

---

## 2) Running the “US model” job files (Fair model site workflow)
If your `fm/` folder contains the Fair model FP files, the official workflow is:

```bat
cd fm
fp.exe > OUT
REM then:
INPUT FILE=FMINPUT.TXT;
```

Then compare the generated `OUT` file to a known-good reference output (often `FMOUT.TXT` or similar), allowing for rounding differences.

---

## 3) Windows command-line recipes
### 3.1 Use Command Prompt (cmd.exe) for easiest redirection
PowerShell behaves differently for stdin redirection; cmd.exe is simplest for `> OUT < IN`.

**Run**
```bat
cd path\to\repo\fm
fp.exe > OUT < IN
```

**Capture exit code**
```bat
echo %ERRORLEVEL%
```

**Run from a specific working directory**
FP tends to use relative filenames. Set the working directory to where the input/output files live:
```bat
cd fm
fp.exe > OUT < IN
```

### 3.2 PowerShell recipes
PowerShell does not support cmd-style `< IN` stdin redirection the same way. Two safe options:

**Option A: call cmd.exe for the run**
```powershell
cd fm
"INPUT FILE=FMINPUT.TXT;" | Set-Content -NoNewline IN
cmd /c "fp.exe > OUT < IN"
```

**Option B: pipe file contents to stdin**
This can work for many console programs:
```powershell
cd fm
"INPUT FILE=FMINPUT.TXT;" | Set-Content IN
Get-Content IN | & .\fp.exe > OUT
```

---

## 4) Linux / CI / containers: running `fp.exe` with Wine
If you’re on Linux and need to run a Windows executable, Wine is the most common approach.

### 4.1 Create an isolated Wine prefix (recommended)
Use a dedicated prefix for fp-wraptr so dependencies/config don’t clash with other Wine apps.

```bash
export WINEPREFIX="$HOME/.wine-fpwraptr"
# Only when FIRST creating the prefix (if fp.exe is 32-bit):
export WINEARCH=win32
winecfg
```

After the prefix exists, you generally omit `WINEARCH` and just keep `WINEPREFIX`:

```bash
export WINEPREFIX="$HOME/.wine-fpwraptr"
wine --version
```

### 4.2 Run FP under Wine (batch mode)
```bash
cd fm
printf "INPUT FILE=FMINPUT.TXT;\n" > IN
/opt/homebrew/bin/wine fp.exe > OUT < IN
```

### 4.3 Headless runners (optional)
If FP unexpectedly needs a display on headless CI, try:
```bash
xvfb-run -a wine fp.exe > OUT < IN
```

### 4.4 Debugging Wine runs
Useful toggles:
```bash
export WINEDEBUG=-all          # reduce noise
# or:
export WINEDEBUG=warn+all      # more diagnostics
```

---

## 5) Scenario/run directory workflows (what wrappers typically do)
In fp-wraptr, your wrapper/CLI will often:
1) Copy baseline model files into a new run directory
2) Patch inputs (exogenous series, parameters, equation overrides, etc.)
3) Run `fp.exe`
4) Collect outputs + produce diffs/plots

### Windows: copy a baseline into a scenario folder
```bat
mkdir runs\scen01
robocopy fm runs\scen01 /E
cd runs\scen01
echo INPUT FILE=FMINPUT.TXT;> IN
fp.exe > OUT < IN
```

### Linux: copy baseline into scenario folder
```bash
mkdir -p runs/scen01
rsync -a fm/ runs/scen01/
cd runs/scen01
printf "INPUT FILE=FMINPUT.TXT;\n" > IN
wine fp.exe > OUT < IN
```

---

## 6) Quick “what does FP expect?” checks
These are useful when wiring automation:

### Check that `fp.exe` exists and is executable
**Windows:**
```bat
dir fm\fp.exe
```

**Linux:**
```bash
ls -l fm/fp.exe
file fm/fp.exe
```

### Generate a command list from inside FP
In an FP input file (or interactively), run:
```text
HELP;
```

---

## 7) Input-file formatting rules (useful when generating files programmatically)
From the FP User’s Guide:
- Commands/options can be upper or lower case.
- Commands end with a semicolon `;`.
- A command can continue on multiple lines; FP reads until it finds a semicolon.
- Multiple commands can be on one line if separated by semicolons.

This matters when writing Python utilities that generate command files.

---

## Sources (official / primary)
Fair model site:
- https://fairmodel.econ.yale.edu/fp/fp.htm  (links to FP User’s Guide and model files)
- https://fairmodel.econ.yale.edu/mmm/mmsecond/fp2012a.pdf  (User’s Guide / manual)
- US model run instructions are also described on the FP page (section “US Model in the FP Program”)

Wine docs:
- `wine(1)` manual page: https://linux.die.net/man/1/wine
- ArchWiki Wine page (good practical examples): https://wiki.archlinux.org/title/Wine
