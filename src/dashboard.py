"""
Rich Terminal Dashboard

Layout:
┌──────────────────────────────────────────────────────────────────────────────┐
│  NIFTY OPTIONS SPIKE DETECTOR  │  Spot: 22,150.25  │  15:23:45  │  TF: [1m] │
├──────────────────────────────────────────────────────────────────────────────┤
│ TRACKED OPTIONS           LTP      Δ%    RSI  RSI-EMA  MACD-H  Status       │
│  ATM CE  NIFTY22150CE    85.50  +2.1%  65.2    61.8    +1.20  ●             │
│  ITM CE  NIFTY22100CE   135.20  +1.8%  68.1    64.5    +1.85  ◉ WATCH       │
│  ATM PE  NIFTY22150PE    78.30  -0.8%  35.4    38.2    -0.90  ●             │
│  ITM PE  NIFTY22200PE   125.60  -1.2%  32.1    35.5    -1.40  ◉ WATCH       │
├──────────────────────────────────────────────────────────────────────────────┤
│ LOOKBACK % MOVE — ATM CE @ 5s  (green=UP spike  red=DOWN spike)             │
│ Bars  │  10    20    30    40    50    60    70    80    90   100  110 ...   │
│ Cumul │ 1.2%  2.1%  2.3%  2.4%  3.1%  3.8%  4.5%  5.2%  5.9%  6.3% ...    │
│ Delta │ 1.2%  0.9%  0.2%  0.1%  0.7%  0.7%  0.7%  0.7%  0.7%  0.4% ...    │
├──────────────────────────────────────────────────────────────────────────────┤
│ SIGNALS  [15:23:41] ATM CE ▲ WATCH — RSI=65.2 MACD↑ Spike=7.1% @20bars    │
│          [15:20:15] ITM PE ▼ EXPIRED                                        │
└──────────────────────────────────────────────────────────────────────────────┘

Keyboard controls:
  [1] 5s timeframe    [2] 15s timeframe    [3] 1m timeframe
  [Tab] cycle focus instrument for lookback table
  [q] quit
"""
import threading
import time
from collections import deque
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

from config import LOOKBACK_PERIODS, TIMEFRAMES, SPIKE_THRESHOLD_PCT
from src.models import Signal, SignalStatus, Indicators, OptionInfo, InstrumentState


# Colour constants
_C = {
    "up":          "bold green",
    "down":        "bold red",
    "neutral":     "white",
    "spike_up":    "black on green",
    "spike_down":  "black on red",
    "watch":       "bold yellow",
    "entry":       "bold bright_green on dark_green",
    "expired":     "dim",
    "header":      "bold cyan",
    "subheader":   "cyan",
    "dim":         "dim white",
    "rsi_ob":      "bold red",       # overbought
    "rsi_os":      "bold green",     # oversold
    "rsi_ok":      "white",
}

_TIMEFRAME_KEYS = list(TIMEFRAMES.keys())   # ["5s", "15s", "1m"]


class Dashboard:
    """
    Rich Live terminal dashboard.

    Call update_state() from the data thread to push new data.
    Call run() from the main thread to start the display loop.
    """

    def __init__(self):
        self._console = Console()
        self._lock = threading.Lock()

        # Display state
        self._active_tf: str = "1m"           # current timeframe (keyboard toggle)
        self._focus_idx: int = 0              # which option's lookback table to show
        self._running: bool = False

        # Shared data (updated by data thread, read by display thread)
        self._nifty_spot: float = 0.0
        self._instrument_states: list[InstrumentState] = []
        self._signals: deque[Signal] = deque(maxlen=30)
        self._status_msg: str = "Initialising…"

    # ── Data-thread API (thread-safe) ──────────────────────────────────────────

    def set_nifty_spot(self, spot: float) -> None:
        with self._lock:
            self._nifty_spot = spot

    def set_instrument_states(self, states: list[InstrumentState]) -> None:
        with self._lock:
            self._instrument_states = states

    def push_signal(self, signal: Signal) -> None:
        with self._lock:
            self._signals.appendleft(signal)

    def set_status(self, msg: str) -> None:
        with self._lock:
            self._status_msg = msg

    # ── Main display loop ──────────────────────────────────────────────────────

    def run(self, refresh_interval: float = 0.5) -> None:
        """Start the Live display. Blocks until 'q' is pressed."""
        self._running = True
        self._start_keyboard_listener()

        with Live(
            self._build_layout(),
            console=self._console,
            refresh_per_second=int(1 / refresh_interval),
            screen=True,
        ) as live:
            while self._running:
                try:
                    live.update(self._build_layout())
                    time.sleep(refresh_interval)
                except KeyboardInterrupt:
                    self._running = False

    def stop(self) -> None:
        self._running = False

    # ── Keyboard listener ─────────────────────────────────────────────────────

    def _start_keyboard_listener(self) -> None:
        """Non-blocking keyboard input thread."""
        def _listen():
            import sys
            import tty
            import termios
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                while self._running:
                    ch = sys.stdin.read(1)
                    if ch == "q":
                        self._running = False
                    elif ch == "1":
                        with self._lock:
                            self._active_tf = "5s"
                    elif ch == "2":
                        with self._lock:
                            self._active_tf = "15s"
                    elif ch == "3":
                        with self._lock:
                            self._active_tf = "1m"
                    elif ch == "\t":   # Tab — cycle focused instrument
                        with self._lock:
                            n = len(self._instrument_states)
                            self._focus_idx = (self._focus_idx + 1) % n if n else 0
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)

        t = threading.Thread(target=_listen, daemon=True)
        t.start()

    # ── Layout construction ────────────────────────────────────────────────────

    def _build_layout(self) -> Layout:
        with self._lock:
            spot = self._nifty_spot
            states = list(self._instrument_states)
            signals = list(self._signals)
            active_tf = self._active_tf
            focus_idx = min(self._focus_idx, len(states) - 1) if states else 0
            status_msg = self._status_msg

        layout = Layout()
        layout.split_column(
            Layout(name="header",   size=3),
            Layout(name="options",  size=9),
            Layout(name="lookback", size=14),
            Layout(name="signals",  size=8),
        )

        layout["header"].update(self._build_header(spot, active_tf, status_msg))
        layout["options"].update(self._build_options_table(states, active_tf))
        layout["lookback"].update(
            self._build_lookback_table(states, focus_idx, active_tf)
        )
        layout["signals"].update(self._build_signals_panel(signals))

        return layout

    # ── Header ─────────────────────────────────────────────────────────────────

    def _build_header(self, spot: float, active_tf: str, status: str) -> Panel:
        spot_str = f"[bold cyan]Spot: ₹{spot:,.2f}[/]" if spot > 0 else "[dim]Spot: --[/]"
        time_str = f"[dim]{datetime.now().strftime('%H:%M:%S IST')}[/]"

        tf_parts = []
        for tf in _TIMEFRAME_KEYS:
            if tf == active_tf:
                tf_parts.append(f"[bold white on blue] {tf} [/]")
            else:
                tf_parts.append(f"[dim] {tf} [/]")
        tf_str = " ".join(tf_parts)

        title = Text.assemble(
            ("  NIFTY OPTIONS SPIKE DETECTOR  ", "bold cyan"),
            ("│ ", "dim"),
        )
        line = Text.assemble(
            ("  NIFTY OPTIONS SPIKE DETECTOR", "bold cyan"),
            ("  │  ", "dim"),
            spot_str,
            ("  │  ", "dim"),
            time_str,
            ("  │  TF: ", "dim"),
            tf_str,
            ("  │  [dim]\\[1]5s [2]15s [3]1m  [Tab]cycle  [q]quit[/]", "dim"),
        )
        return Panel(line, style="on black", padding=(0, 1))

    # ── Options overview table ─────────────────────────────────────────────────

    def _build_options_table(self, states: list[InstrumentState], active_tf: str) -> Panel:
        t = Table(
            box=box.SIMPLE_HEAD,
            expand=True,
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
        )
        t.add_column("Label",    style="bold white", width=9)
        t.add_column("Symbol",   style="dim white",  width=22)
        t.add_column("LTP",      justify="right",    width=9)
        t.add_column("Δ%",       justify="right",    width=8)
        t.add_column("RSI",      justify="right",    width=7)
        t.add_column("RSI-EMA",  justify="right",    width=9)
        t.add_column("MACD-H",   justify="right",    width=9)
        t.add_column("Bars",     justify="right",    width=6)
        t.add_column("Status",   width=16)

        for st in states:
            ind: Optional[Indicators] = st.indicators.get(active_tf)
            if ind is None:
                ind = Indicators()

            ltp_str = f"{st.ltp:.2f}" if st.ltp > 0 else "---"
            delta_pct = st.ltp_change_pct
            delta_str = f"{delta_pct:+.2f}%" if st.ltp > 0 else "---"
            delta_style = _C["up"] if delta_pct > 0 else (_C["down"] if delta_pct < 0 else _C["neutral"])

            rsi = ind.rsi
            rsi_style = _C["rsi_ob"] if rsi >= 70 else (_C["rsi_os"] if rsi <= 30 else _C["rsi_ok"])

            macd_h = ind.macd_hist
            macd_str = f"{macd_h:+.3f}" if macd_h != 0 else " 0.000"
            macd_style = _C["up"] if macd_h > 0 else (_C["down"] if macd_h < 0 else _C["neutral"])

            # Signal status indicator
            active_sig = st.active_signals[-1] if st.active_signals else None
            if active_sig and active_sig.timeframe == active_tf:
                status_style, status_icon = {
                    SignalStatus.SPIKE:   (_C["neutral"],  "◌ SPIKE"),
                    SignalStatus.WATCH:   (_C["watch"],    "◉ WATCH"),
                    SignalStatus.ENTRY:   (_C["entry"],    "★ ENTRY"),
                    SignalStatus.EXPIRED: (_C["expired"],  "  ──"),
                }.get(active_sig.status, (_C["neutral"], "●"))
            else:
                status_style, status_icon = _C["neutral"], "●"

            from src.bar_builder import BarBuilder
            # Bar count from state
            bars_str = str(len(st.bars.get(active_tf, [])))

            t.add_row(
                st.info.label,
                st.info.symbol[:22],
                ltp_str,
                Text(delta_str, style=delta_style),
                Text(f"{rsi:.1f}", style=rsi_style),
                f"{ind.rsi_ema:.1f}",
                Text(macd_str, style=macd_style),
                bars_str,
                Text(status_icon, style=status_style),
            )

        return Panel(t, title="[bold cyan]TRACKED OPTIONS[/]", border_style="cyan", padding=0)

    # ── Lookback % move table ──────────────────────────────────────────────────

    def _build_lookback_table(
        self, states: list[InstrumentState], focus_idx: int, active_tf: str
    ) -> Panel:
        """
        The key visual: shows cumulative % and delta (acceleration) for all
        lookback periods. Cells are colour-coded by spike magnitude.

        Rows per instrument:
          Cumul: % from N bars ago to now  (positive=price higher than N bars ago)
          Delta: difference between consecutive lookbacks (shows WHERE spike was)
        """
        if not states:
            return Panel("[dim]No instruments loaded yet[/]", title="LOOKBACK % MOVE")

        # Build column headers
        headers = ["Instrument / Bars →"] + [str(lb) for lb in LOOKBACK_PERIODS]

        t = Table(
            box=box.SIMPLE,
            expand=True,
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
            padding=(0, 0),
        )
        t.add_column(headers[0], width=20, no_wrap=True)
        for h in headers[1:]:
            t.add_column(h, justify="right", width=7, no_wrap=True)

        for i, st in enumerate(states):
            ind: Optional[Indicators] = st.indicators.get(active_tf)
            if ind is None:
                ind = Indicators()

            is_focused = (i == focus_idx)
            label_prefix = "▶ " if is_focused else "  "
            label_style = "bold white" if is_focused else "dim white"

            # ── Cumulative % row ─────────────────────────────────────────────
            cumul_cells = [Text(f"{label_prefix}{st.info.label} Cumul", style=label_style)]
            for lb in LOOKBACK_PERIODS:
                val = ind.lookback_pct.get(lb)
                if val is None:
                    cumul_cells.append(Text("  ---", style="dim"))
                else:
                    s = f"{val:+.1f}%"
                    style = _C["up"] if val > 0 else (_C["down"] if val < 0 else _C["neutral"])
                    cumul_cells.append(Text(s, style=style))
            t.add_row(*cumul_cells)

            # ── Delta (acceleration) row — THIS reveals the spike ────────────
            delta_cells = [Text(f"  {st.info.label} Delta", style="dim")]
            for lb in LOOKBACK_PERIODS:
                d = ind.lookback_delta.get(lb)
                if d is None:
                    delta_cells.append(Text("  ---", style="dim"))
                else:
                    s = f"{d:+.1f}%"
                    if abs(d) >= SPIKE_THRESHOLD_PCT:
                        # Spike! Highlight the cell
                        bg = "green" if d > 0 else "red"
                        style = f"bold black on {bg}"
                    elif abs(d) >= SPIKE_THRESHOLD_PCT * 0.6:
                        style = "bold yellow"
                    else:
                        style = "dim white"
                    delta_cells.append(Text(s, style=style))
            t.add_row(*delta_cells)

            # Separator between instruments
            if i < len(states) - 1:
                t.add_row(*[""] * len(headers))

        focused_label = states[focus_idx].info.label if states else "?"
        title = (
            f"[bold cyan]LOOKBACK % MOVE[/] — [yellow]{active_tf}[/] bars  "
            f"[dim]│ ▶={focused_label}  [Tab] to cycle  │  "
            f"[green]■[/]=UP spike [red]■[/]=DOWN spike  threshold≥{SPIKE_THRESHOLD_PCT:.0f}%[/]"
        )
        return Panel(t, title=title, border_style="cyan", padding=0)

    # ── Signals panel ──────────────────────────────────────────────────────────

    def _build_signals_panel(self, signals: list[Signal]) -> Panel:
        lines: list[Text] = []

        if not signals:
            lines.append(Text("  No signals yet — monitoring for spikes…", style="dim"))
        else:
            for sig in signals[:8]:   # show last 8 signals
                ts = sig.timestamp.strftime("%H:%M:%S")
                dir_sym = "▲" if sig.direction == "UP" else "▼"
                dir_style = _C["up"] if sig.direction == "UP" else _C["down"]

                status_style = {
                    SignalStatus.SPIKE:   "yellow",
                    SignalStatus.WATCH:   "bold yellow",
                    SignalStatus.ENTRY:   "bold bright_green",
                    SignalStatus.EXPIRED: "dim",
                }.get(sig.status, "white")

                line = Text()
                line.append(f"  [{ts}] ", style="dim")
                line.append(f"{sig.label:<9}", style="bold white")
                line.append(f" {dir_sym} ", style=dir_style)
                line.append(f"{sig.timeframe:<4}", style="cyan")
                line.append(f" {sig.status:<8}", style=status_style)
                line.append(f" │ {sig.message}", style="dim white")
                lines.append(line)

        from rich.columns import Columns
        content = "\n".join(str(l) for l in lines)

        # Render using Text objects directly
        table = Table(box=None, show_header=False, expand=True, padding=(0, 0))
        table.add_column("", no_wrap=False)
        for line in lines:
            table.add_row(line)

        return Panel(
            table,
            title="[bold cyan]SIGNALS[/] [dim]│ ▲=UP  ▼=DOWN[/]",
            border_style="cyan",
            padding=0,
        )
