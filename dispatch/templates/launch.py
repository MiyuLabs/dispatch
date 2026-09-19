from __future__ import annotations

from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .base import Template
from ..models import Recipient, RenderedEmail

_TEMPLATE_DIR = Path(__file__).resolve().parent / "html"


class LaunchTemplate(Template):
    """The waitlist -> "we're live, here's your pre-order discount" email.

    All copy that might change between sends (URLs, discount amounts, an
    optional promo code) is passed in at construction time rather than
    hardcoded, so re-running a future campaign with different numbers
    doesn't require touching this file. Per-recipient personalization
    (currently just an optional display name) is read from
    `recipient.metadata` — add more Source fields and reference them in
    `render()` when MiyuLabs needs it.
    """

    name = "launch"

    def __init__(
        self,
        pre_order_url: str = "https://miyulabs.in/",
        explore_url: Optional[str] = None,
        solo_mrp: str = "14,999",
        solo_price: str = "5,999",
        partner_mrp: str = "29,999",
        partner_price: str = "11,999",
        partner_discount: str = "1,000",
        preorder_discount: str = "500",
        promo_code: Optional[str] = None,
        **kwargs,
    ):
        self.pre_order_url = pre_order_url
        self.explore_url = explore_url or f"{pre_order_url.rstrip('/')}/explore"
        
        self.solo_mrp = solo_mrp
        self.solo_price = solo_price
        self.partner_mrp = partner_mrp
        self.partner_price = partner_price
        self.partner_discount = partner_discount
        self.preorder_discount = preorder_discount
        
        def _parse(val: str) -> int:
            return int(val.replace(",", "").replace("₹", ""))
        def _fmt(val: int) -> str:
            return f"₹{val:,}"

        try:
            self.solo_effective = _fmt(_parse(solo_price) - _parse(preorder_discount))
            partner_base = _parse(partner_price) - _parse(partner_discount)
            self.partner_bundle_price = _fmt(partner_base)
            self.partner_effective = _fmt(partner_base - _parse(preorder_discount))
        except ValueError:
            self.solo_effective = solo_price
            self.partner_bundle_price = partner_price
            self.partner_effective = partner_price

        self.promo_code = promo_code
        self._env = Environment(
            loader=FileSystemLoader(str(_TEMPLATE_DIR)),
            autoescape=select_autoescape(["html"]),
        )
        self._html_template = self._env.get_template("launch_email.html")

    def render(self, recipient: Recipient) -> RenderedEmail:
        ctx = {
            "pre_order_url": self.pre_order_url,
            "explore_url": self.explore_url,
            "preorder_discount": f"₹{self.preorder_discount}" if not self.preorder_discount.startswith("₹") else self.preorder_discount,
            "partner_discount": f"₹{self.partner_discount}" if not self.partner_discount.startswith("₹") else self.partner_discount,
            "solo_mrp": f"₹{self.solo_mrp}" if not self.solo_mrp.startswith("₹") else self.solo_mrp,
            "solo_price": f"₹{self.solo_price}" if not self.solo_price.startswith("₹") else self.solo_price,
            "solo_effective": self.solo_effective,
            "partner_mrp": f"₹{self.partner_mrp}" if not self.partner_mrp.startswith("₹") else self.partner_mrp,
            "partner_price": f"₹{self.partner_price}" if not self.partner_price.startswith("₹") else self.partner_price,
            "partner_bundle_price": self.partner_bundle_price,
            "partner_effective": self.partner_effective,
            "promo_code": self.promo_code,
        }
        return RenderedEmail(
            subject="Miyu is here. 🌙",
            html=self._html_template.render(**ctx),
            text=self._render_text(ctx),
        )

    @staticmethod
    def _render_text(ctx: dict) -> str:
        lines = [
            "MiyuLabs",
            "",
            "Miyu is here. 🌙",
            "Your little desk companion is ready to come home.",
            "",
            "Hey,",
            "",
            "You joined the waitlist a while ago. Well...",
            "Miyu is finally here.",
            "",
            "She's a little desk companion built for the hours you spend at your desk —",
            "whether you're deep in work, listening to something at 2am, taking a break,",
            "or just staring at the ceiling pretending you're about to be productive.",
            "",
            "She keeps time, helps you focus, plays music, reacts when you touch her,",
            "fills the desk with tiny ambient scenes, and — if you pair two Miyus —",
            "lets someone you care about quietly be there too.",
            "",
            "And because you were here before all of that was real, we've got something for you.",
            "",
            "FOR THE FIRST 100",
            "A little thank-you for getting here early.",
            "",
            "--------------------------------------------------",
            "SOLO PACK",
            "1x Miyu Desk Companion",
            f"MRP: {ctx['solo_mrp']}",
            f"Launch: {ctx['solo_price']}",
            f"Early pre-order: -{ctx['preorder_discount']}",
            f"Effective Price: {ctx['solo_effective']}",
            "",
            "PARTNER PACK",
            "2x Miyu Units",
            f"MRP: {ctx['partner_mrp']}",
            f"Base: {ctx['partner_price']}",
            f"Partner: -{ctx['partner_discount']}",
            f"Early pre-order: -{ctx['preorder_discount']}",
            f"Effective Price: {ctx['partner_effective']}",
            "--------------------------------------------------",
        ]

        if ctx["promo_code"]:
            lines += [
                "",
                "USE CODE",
                ctx["promo_code"],
            ]

        lines += [
            "",
            "A LITTLE MORE THAN A SCREEN",
            "",
            "♡ Reacts to you",
            "✦ Ambient scenes",
            "⌁ Productivity tools like Pomodoro & todos",
            "♡ Partner pairing & messages",
            "♪ Lofi / focus music",
            "",
            f"Pre-order Miyu → {ctx['pre_order_url']}",
            "",
            f"Meet Miyu properly first → {ctx['explore_url']}",
            "",
            "— MiyuLabs",
            "Building an aesthetic world for the hours that feel a little lonely.",
            "",
            "Made with ♡ and sleepless nights.",
        ]

        return "\n".join(lines)
