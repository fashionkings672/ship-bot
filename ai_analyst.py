"""
ai_analyst.py — AI-powered performance analysis (FINAL)
"""

import os, logging
import openai
from datetime import date, timedelta

log = logging.getLogger("ai_analyst")

openai.api_key = os.getenv("OPENAI_API_KEY")

async def analyze_creative_performance(creative_stats, days=7):
    """
    Analyze creative performance and provide recommendations.
    
    Args:
        creative_stats: dict of creative -> {orders, revenue, advance_paid, full_cod, pending}
        days: period in days
    
    Returns:
        AI analysis string
    """
    if not openai.api_key:
        return None
    
    try:
        # Prepare data for AI
        data_summary = []
        for creative, stats in sorted(creative_stats.items(), key=lambda x: -x[1]['orders']):
            total = stats['orders']
            conv_rate = round((stats['advance_paid'] + stats['full_cod']) / total * 100, 1) if total else 0
            
            data_summary.append({
                'creative': creative,
                'orders': total,
                'revenue': stats['revenue'],
                'conversion_rate': conv_rate,
                'advance_paid': stats['advance_paid'],
                'full_cod': stats['full_cod'],
                'pending': stats['pending'],
            })
        
        prompt = f"""
Analyze this ad creative performance data from the last {days} days and provide recommendations:

{data_summary}

Rules for analysis:
- Conversion rate (advance + full cod) >= 60%: Excellent - Scale up
- Conversion rate 40-60%: Good - Maintain or optimize
- Conversion rate < 40%: Poor - Need changes or kill

Provide:
1. Best performers (recommend scale)
2. Moderate performers (optimization tips)
3. Poor performers (kill or major changes)
4. Overall actionable insights

Keep response under 500 characters, bullet points, be direct.
"""
        
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        log.error(f"AI analysis error: {e}")
        return None

async def generate_daily_briefing(orders):
    """
    Generate AI-powered daily briefing.
    
    Args:
        orders: all orders from Google Sheets
    
    Returns:
        Daily briefing text
    """
    if not openai.api_key:
        return "🤖 AI briefing unavailable (no API key)"
    
    try:
        from collections import defaultdict
        
        today = date.today().isoformat()
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        
        today_orders = [o for o in orders if o['date'].startswith(today)]
        yesterday_orders = [o for o in orders if o['date'].startswith(yesterday)]
        week_orders = [o for o in orders if o['date'] >= week_ago]
        
        # Creative breakdown
        creative_stats = defaultdict(lambda: {'orders': 0, 'conv': 0})
        for o in yesterday_orders:
            creative = o.get('creative', '—')
            creative_stats[creative]['orders'] += 1
            if o['advance_paid'] or o['advance_paid'] == 0:
                creative_stats[creative]['conv'] += 1
        
        # Prepare summary
        summary = {
            'today_orders': len(today_orders),
            'yesterday_orders': len(yesterday_orders),
            'week_orders': len(week_orders),
            'creative_breakdown': dict(creative_stats)
        }
        
        prompt = f"""
Generate a brief daily performance summary for an e-commerce business:

Summary data:
{summary}

Provide:
1. Key highlights from yesterday
2. Best performing creative (if any)
3. Areas of concern
4. 2-3 action items for today

Keep response under 600 characters, use emojis, be actionable and direct.
"""
        
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.4
        )
        
        briefing = f"🤖 *AI DAILY BRIEFING - {date.today()}*\n\n"
        briefing += response.choices[0].message.content.strip()
        
        return briefing
        
    except Exception as e:
        log.error(f"Daily briefing error: {e}")
        return f"🤖 AI briefing error: {e}"
