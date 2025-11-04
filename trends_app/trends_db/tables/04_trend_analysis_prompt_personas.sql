-- ============================================================================
-- TREND ANALYSIS PROMPT TEMPLATES
-- ============================================================================

-- 1. CUSTOMER SUCCESS / ACCOUNT MANAGEMENT PERSONA
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Customer Success Health Review',
  'trend_analysis',
  E'You are a Customer Success Manager analyzing {days}-day health trends for {company_name}.

## Account Overview
- Health check runs: {total_runs}
- Technologies: {technologies}
- Period: {first_run} to {last_run}

## Recurring Issues
{recurring_issues_section}

## Health Score Trend
{health_score_section}

## Cross-Technology Patterns
{cross_tech_section}

## Your Task
Create a customer success health review focusing on:

1. **Account Health Status**:
   - Overall health rating (Green/Yellow/Red)
   - Trajectory: Improving, Stable, or Degrading?
   - Churn risk assessment (Low/Medium/High)

2. **Customer Impact Analysis**:
   - Which issues are likely causing customer pain?
   - Are they aware of these problems?
   - Estimated impact on their business operations

3. **Proactive Outreach Strategy**:
   - What conversation should we have with the customer?
   - Best approach (email, call, QBR agenda item)?
   - Who should we contact (technical contact vs. executive sponsor)?

4. **Upsell/Expansion Opportunities**:
   - Resource constraints requiring upgrades?
   - Additional services they need (consulting, training)?
   - Cross-sell opportunities (new technologies)?

5. **Action Items**:
   - Immediate actions (next 7 days)
   - Short-term follow-ups (30 days)
   - Long-term relationship building

6. **Success Metrics**:
   - What does "resolved" look like?
   - How will we measure improvement?

Format in AsciiDoc with clear action items and talking points for customer conversations.',
  NULL
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();


-- 2. EXECUTIVE SUMMARY PERSONA (C-SUITE / VP LEVEL)
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Executive Business Impact Report',
  'trend_analysis',
  E'You are a CTO analyzing {days}-day infrastructure health data for {company_name}.

## Infrastructure Overview
- Monitoring period: {days} days
- Health checks: {total_runs}
- Technologies: {technologies}
- Period: {first_run} to {last_run}

## Issues Summary
{recurring_issues_section}

## Performance Trend
{health_score_section}

## Technology Stack Health
{cross_tech_section}

## Your Task
Create an executive summary focused on business impact:

1. **Executive Summary** (3-4 sentences max):
   - Current state in plain English
   - Most critical finding
   - Recommended action

2. **Business Risk Assessment**:
   - Quantified risk: Likelihood × Impact
   - Potential revenue impact ($)
   - Operational disruption scenarios
   - Compliance or SLA exposure

3. **Financial Impact**:
   - Current waste/inefficiency costs ($ per month)
   - Incident prevention value
   - Optimization opportunity ($)
   - ROI of recommended actions

4. **Resource Implications**:
   - Engineering time required
   - Budget needed for remediation
   - Timeline to resolution

5. **Strategic Recommendations** (Max 3):
   - Priority ranked actions
   - Investment required
   - Expected outcomes

6. **Decision Required**:
   - What needs executive approval?
   - Timeline for decision
   - Cost of inaction

Keep language non-technical. Focus on business outcomes, financials, and risks. Use AsciiDoc format.',
  NULL
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();


-- 3. TECHNICAL DEEP DIVE PERSONA (ENGINEERING LEADERSHIP)
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Technical Root Cause Analysis',
  'trend_analysis',
  E'You are a Staff Database Engineer performing deep technical analysis of {days}-day trends for {company_name}.

## System Profile
- Analysis window: {days} days ({first_run} to {last_run})
- Health check runs: {total_runs}
- Technology stack: {technologies}

## Detected Issues
{recurring_issues_section}

## Health Metrics
{health_score_section}

## Cross-System Patterns
{cross_tech_section}

## Your Task
Provide a technical deep-dive analysis:

1. **Root Cause Analysis**:
   - Primary root causes (not just symptoms)
   - Contributing factors
   - Why these issues persist/recur
   - Technical debt implications

2. **System Architecture Assessment**:
   - Configuration anti-patterns identified
   - Resource allocation issues
   - Scalability constraints
   - Performance bottlenecks

3. **Issue Correlation Analysis**:
   - How are different issues related?
   - Cascading failure risks
   - Cross-technology dependencies
   - Hidden coupling points

4. **Technical Remediation Plan**:
   - Immediate fixes (< 1 week)
   - Short-term improvements (1-4 weeks)
   - Long-term architectural changes (1-3 months)
   - For each: specific commands/configs/code changes

5. **Prevention Strategy**:
   - Monitoring gaps to address
   - Alerting rules to implement
   - Configuration changes needed
   - Process improvements

6. **Testing & Validation Plan**:
   - How to verify fixes work
   - Rollback procedures
   - Performance benchmarks
   - Success criteria

Be highly technical. Include specific commands, configuration examples, and architectural diagrams (in AsciiDoc format). Cite documentation where relevant.',
  NULL
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();


-- 4. SALES ENABLEMENT PERSONA
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Competitive Win Strategy',
  'trend_analysis',
  E'You are a Sales Engineer analyzing {days}-day trend data for prospect/customer {company_name}.

## Account Intelligence
- Technologies in use: {technologies}
- Health checks performed: {total_runs}
- Analysis period: {first_run} to {last_run}

## Issues Detected
{recurring_issues_section}

## Health Trajectory
{health_score_section}

## Technology Stack Analysis
{cross_tech_section}

## Your Task
Create a sales strategy document:

1. **Value Proposition**:
   - What makes our platform better for their specific issues?
   - Proof points (what we detected that they likely didn\'t know)
   - Competitive differentiation

2. **Pain Points Identified**:
   - Technical pain (for engineers)
   - Business pain (for executives)
   - Operational pain (for ops teams)
   - What keeps them up at night?

3. **Objection Handling**:
   - Likely objections based on their current state
   - Pre-emptive responses with data
   - Competitive positioning vs. AWS/Azure/GCP

4. **ROI Narrative**:
   - Cost of their current issues (quantified)
   - Value of our solution (specific to their problems)
   - Payback period
   - Risk mitigation value

5. **Deal Acceleration Strategy**:
   - Urgency triggers (why act now?)
   - Champion building (who to engage)
   - Proof of value (pilot/POC approach)
   - Expansion opportunities

6. **Talking Points** (Max 5):
   - Executive level (business outcomes)
   - Technical level (architecture/performance)
   - Operations level (easier management)

7. **Next Steps**:
   - Proposed engagement path
   - Timeline to close
   - Success metrics

Make this actionable for a sales team. Focus on winning the deal. Use AsciiDoc format with clear sections.',
  NULL
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();


-- 5. SUPPORT TRIAGE PERSONA
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Support Escalation Guide',
  'trend_analysis',
  E'You are a Support Engineering Lead triaging issues for {company_name} based on {days} days of health data.

## Customer Environment
- Technologies: {technologies}
- Health checks: {total_runs}
- Period: {first_run} to {last_run}

## Active Issues
{recurring_issues_section}

## System Health
{health_score_section}

## Cross-Technology Context
{cross_tech_section}

## Your Task
Create a support triage guide:

1. **Severity Classification**:
   - P0 (Critical): Immediate escalation required
   - P1 (High): Escalate within 4 hours
   - P2 (Medium): Standard support queue
   - P3 (Low): Self-service/documentation

2. **Known Issues Mapping**:
   - Match detected issues to known bugs/limitations
   - Link to internal KB articles
   - Workarounds available
   - ETA for permanent fixes

3. **Escalation Criteria**:
   - When to involve engineering
   - When to engage customer success
   - When to alert account management
   - When to notify product team

4. **Customer Communication Templates**:
   - What to tell customer about each issue
   - Expected resolution timeline
   - Workaround instructions
   - Proactive communication (before they open ticket)

5. **Troubleshooting Playbook**:
   - For each major issue: diagnostic steps
   - Data to collect from customer
   - Common misconfigurations
   - Quick wins vs. long-term fixes

6. **Prevention Guidance**:
   - Configuration recommendations
   - Monitoring/alerting setup
   - Best practices to share
   - Documentation to provide

7. **Metrics Tracking**:
   - Which issues are trending up?
   - Repeat offenders (by customer or issue type)
   - MTTR by issue category
   - Support ticket correlation

Format in AsciiDoc with clear action codes (P0/P1/P2/P3) and linked procedures. Make it scannable for busy support engineers.',
  NULL
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();


-- 6. PLATFORM INSIGHTS PERSONA (PRODUCT MANAGEMENT)
INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (
  'Platform Health Insights',
  'trend_analysis',
  E'You are a Product Manager analyzing aggregated trend data across multiple customers. This report is for {company_name} as a representative sample.

## Customer Data Point
- Customer: {company_name}
- Technologies: {technologies}
- Analysis period: {days} days ({first_run} to {last_run})
- Health checks: {total_runs}

## Issues Detected
{recurring_issues_section}

## Health Trend
{health_score_section}

## Technology Stack
{cross_tech_section}

## Your Task
Extract platform-wide insights:

1. **Pattern Recognition**:
   - Is this issue unique to this customer or systemic?
   - Which issues appear across multiple customers?
   - Technology-specific patterns (e.g., "all Kafka 3.5 users hit this")
   - Configuration anti-patterns we should address

2. **Product Gaps Identified**:
   - Missing features causing these issues
   - Configuration defaults that need changing
   - Monitoring/alerting gaps
   - Documentation deficiencies

3. **Platform Improvements Needed**:
   - Auto-remediation opportunities
   - Better defaults to implement
   - New health checks to add
   - UI/UX improvements for customer self-service

4. **Competitive Analysis**:
   - Which issues do competitors handle better?
   - Unique problems to our platform
   - Differentiation opportunities

5. **Customer Education Needs**:
   - What do customers not understand?
   - Documentation gaps
   - Training opportunities
   - Best practices to publish

6. **Roadmap Implications**:
   - Feature priorities based on pain points
   - Technical debt to address
   - Investment areas (where to focus engineering)

7. **Metrics to Track**:
   - What should we measure going forward?
   - Success criteria for improvements
   - Leading indicators of platform health

Focus on actionable insights for product strategy. Use AsciiDoc format.',
  NULL
)
ON CONFLICT (template_name) DO UPDATE 
  SET template_content = EXCLUDED.template_content,
      updated_at = now();
