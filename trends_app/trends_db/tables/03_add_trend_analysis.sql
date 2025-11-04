-- File: trends_db/migrations/03_add_trend_analysis.sql

-- 1. Add privilege

SELECT createpriv('trend_analysis', 'ViewTrendAnalysis', 'Access trend analysis and engagement recommendations');

-- 2. Extend generated_ai_reports table
ALTER TABLE generated_ai_reports 
  ADD COLUMN IF NOT EXISTS report_type VARCHAR(50) DEFAULT 'health_check',
  ADD COLUMN IF NOT EXISTS company_id INTEGER REFERENCES companies(id),
  ADD COLUMN IF NOT EXISTS analysis_period_days INTEGER,
  ADD COLUMN IF NOT EXISTS analysis_persona VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_generated_reports_type ON generated_ai_reports(report_type);
CREATE INDEX IF NOT EXISTS idx_generated_reports_company ON generated_ai_reports(company_id);

-- 3. Add consulting engagement prompt template
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Engagement Recommendation Analysis',
  'trend_analysis',
  E'You are an expert database consultant analyzing {days}-day trend data for {company_name}.

## Company Overview
- Total health check runs: {total_runs}
- Technologies monitored: {technologies}
- Analysis period: {first_run} to {last_run}

## Recurring Issues
{recurring_issues_section}

## Health Score Trend
{health_score_section}

## Cross-Technology Patterns
{cross_tech_section}

## Your Task
Provide a consulting engagement recommendation with:

1. **Pattern Analysis**: What\'s happening? Identify key trends and correlations.

2. **Risk Assessment**: Urgency level (LOW/MEDIUM/HIGH/CRITICAL) and potential business impact.

3. **Recommended Engagement**:
   - Engagement Type: (Architecture Review / Migration / Optimization / Emergency Response)
   - Scope: Specific areas to address
   - Duration: Estimated timeline
   - Priority: When should this start?

4. **Expected Business Value**:
   - Risk mitigation (incidents prevented)
   - Cost savings estimate
   - Infrastructure improvements
   - Additional opportunities (cross-sell)

5. **Key Talking Points**: 3-5 bullet points for client conversation.

Format your response in AsciiDoc with clear headers and actionable recommendations.',
  NULL  -- system-wide template, not user-specific
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();
