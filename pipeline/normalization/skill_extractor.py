"""
Rule-based skill extractor.

Scans job description text for canonical skill names from a curated taxonomy.
Returns (skill_name, is_required, confidence_score).

Phase 2 upgrade path: replace the LLM_FALLBACK stub with an Anthropic API call
for descriptions that score low confidence on the rule-based pass.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# ── Skills taxonomy ───────────────────────────────────────────────────────────
# Format: { canonical_name: [aliases], category }

SKILLS_TAXONOMY: dict[str, dict] = {

    # ════════════════════════════════════════════════════════
    # TECH TOOLS / INFRA
    # ════════════════════════════════════════════════════════
    "dbt":              {"aliases": ["dbt", "dbt-core", "dbt labs", "data build tool"], "category": "tool"},
    "Apache Spark":     {"aliases": ["spark", "pyspark", "apache spark"],        "category": "tool"},
    "Apache Kafka":     {"aliases": ["kafka", "apache kafka", "kafka streams"],  "category": "tool"},
    "Apache Airflow":   {"aliases": ["airflow", "apache airflow"],               "category": "tool"},
    "Apache Flink":     {"aliases": ["flink", "apache flink"],                   "category": "tool"},
    "Kubernetes":       {"aliases": ["k8s", "kubernetes"],                       "category": "tool"},
    "Docker":           {"aliases": ["docker", "dockerfile", "containerization"],"category": "tool"},
    "Terraform":        {"aliases": ["terraform", "hashicorp terraform"],        "category": "tool"},
    "MLflow":           {"aliases": ["mlflow", "ml flow"],                       "category": "tool"},
    "Ray":              {"aliases": ["ray", "ray tune", "ray serve"],            "category": "tool"},
    "Databricks":       {"aliases": ["databricks", "delta lake"],                "category": "tool"},
    "Snowflake":        {"aliases": ["snowflake"],                               "category": "tool"},
    "BigQuery":         {"aliases": ["bigquery", "google bigquery"],             "category": "tool"},
    "Redshift":         {"aliases": ["redshift", "amazon redshift"],             "category": "tool"},
    "Redis":            {"aliases": ["redis", "redis cache"],                    "category": "tool"},
    "Elasticsearch":    {"aliases": ["elasticsearch", "elastic", "opensearch"],  "category": "tool"},
    "Triton":           {"aliases": ["triton", "nvidia triton"],                 "category": "tool"},
    "AWS SageMaker":    {"aliases": ["sagemaker", "aws sagemaker"],              "category": "tool"},
    "Vertex AI":        {"aliases": ["vertex ai", "google vertex"],              "category": "tool"},
    "PostgreSQL":       {"aliases": ["postgresql", "postgres"],                  "category": "tool"},
    "MySQL":            {"aliases": ["mysql"],                                   "category": "tool"},
    "MongoDB":          {"aliases": ["mongodb", "mongo"],                        "category": "tool"},
    "GitHub":           {"aliases": ["github", "git"],                          "category": "tool"},
    "GitLab":           {"aliases": ["gitlab"],                                  "category": "tool"},

    # ════════════════════════════════════════════════════════
    # PROGRAMMING LANGUAGES
    # ════════════════════════════════════════════════════════
    "Python":           {"aliases": ["python", "python3"],                       "category": "technical"},
    "SQL":              {"aliases": ["sql", "t-sql", "pl/sql"],                  "category": "technical"},
    "Go":               {"aliases": [" go ", "golang"],                          "category": "technical"},
    "Java":             {"aliases": ["java"],                                    "category": "technical"},
    "Rust":             {"aliases": ["rust", "rustlang"],                        "category": "technical"},
    "TypeScript":       {"aliases": ["typescript"],                              "category": "technical"},
    "JavaScript":       {"aliases": ["javascript", "node.js"],                   "category": "technical"},
    "R":                {"aliases": [" r ", "r language", "rstudio"],            "category": "technical"},
    "C++":              {"aliases": ["c++", "cpp"],                              "category": "technical"},
    "Scala":            {"aliases": ["scala"],                                   "category": "technical"},
    "Swift":            {"aliases": ["swift"],                                   "category": "technical"},
    "Kotlin":           {"aliases": ["kotlin"],                                  "category": "technical"},
    "PHP":              {"aliases": ["php"],                                     "category": "technical"},

    # ════════════════════════════════════════════════════════
    # ML / DS LIBRARIES
    # ════════════════════════════════════════════════════════
    "PyTorch":          {"aliases": ["pytorch", "torch"],                        "category": "technical"},
    "TensorFlow":       {"aliases": ["tensorflow", "keras"],                     "category": "technical"},
    "scikit-learn":     {"aliases": ["scikit-learn", "sklearn"],                 "category": "technical"},
    "pandas":           {"aliases": ["pandas"],                                  "category": "technical"},
    "NumPy":            {"aliases": ["numpy"],                                   "category": "technical"},
    "CUDA":             {"aliases": ["cuda", "nvidia cuda"],                     "category": "technical"},
    "LangChain":        {"aliases": ["langchain", "lang chain"],                 "category": "technical"},
    "RAG":              {"aliases": ["retrieval augmented generation", " rag "], "category": "technical"},
    "LLM":              {"aliases": ["large language model", " llm ", "llms"],   "category": "domain"},

    # ════════════════════════════════════════════════════════
    # CLOUD
    # ════════════════════════════════════════════════════════
    "AWS":              {"aliases": ["aws", "amazon web services"],              "category": "tool"},
    "GCP":              {"aliases": ["gcp", "google cloud"],                     "category": "tool"},
    "Azure":            {"aliases": ["azure", "microsoft azure"],                "category": "tool"},

    # ════════════════════════════════════════════════════════
    # TECH CONCEPTS / METHODS
    # ════════════════════════════════════════════════════════
    "Machine Learning": {"aliases": ["machine learning", "deep learning"],       "category": "domain"},
    "A/B Testing":      {"aliases": ["a/b testing", "a/b test", "experimentation"], "category": "domain"},
    "Statistics":       {"aliases": ["statistics", "statistical modeling", "probability"], "category": "domain"},
    "REST APIs":        {"aliases": ["rest api", "rest apis", "restful"],        "category": "domain"},
    "GraphQL":          {"aliases": ["graphql"],                                 "category": "domain"},
    "gRPC":             {"aliases": ["grpc"],                                    "category": "domain"},
    "Microservices":    {"aliases": ["microservices", "microservice"],           "category": "domain"},
    "CI/CD":            {"aliases": ["ci/cd", "continuous integration", "continuous deployment", "github actions", "jenkins"], "category": "domain"},
    "System Design":    {"aliases": ["system design", "distributed systems"],   "category": "domain"},

    # ════════════════════════════════════════════════════════
    # DESIGN TOOLS & SKILLS
    # ════════════════════════════════════════════════════════
    "Figma":            {"aliases": ["figma"],                                   "category": "tool"},
    "Adobe XD":         {"aliases": ["adobe xd", "xd"],                         "category": "tool"},
    "Sketch":           {"aliases": ["sketch app", "sketch design"],             "category": "tool"},
    "Adobe Illustrator":{"aliases": ["illustrator", "adobe illustrator"],        "category": "tool"},
    "Adobe Photoshop":  {"aliases": ["photoshop", "adobe photoshop"],            "category": "tool"},
    "InVision":         {"aliases": ["invision"],                                "category": "tool"},
    "Prototyping":      {"aliases": ["prototyping", "prototype", "wireframing", "wireframe"], "category": "domain"},
    "UX Design":        {"aliases": ["ux design", "user experience design", "interaction design"], "category": "domain"},
    "UI Design":        {"aliases": ["ui design", "user interface design"],      "category": "domain"},
    "Design Systems":   {"aliases": ["design system", "design systems", "component library"], "category": "domain"},
    "Accessibility":    {"aliases": ["accessibility", "wcag", "a11y"],          "category": "domain"},
    "Typography":       {"aliases": ["typography"],                              "category": "domain"},
    "Motion Design":    {"aliases": ["motion design", "animation", "after effects"], "category": "domain"},

    # ════════════════════════════════════════════════════════
    # MARKETING TOOLS & SKILLS
    # ════════════════════════════════════════════════════════
    "Google Analytics": {"aliases": ["google analytics", "ga4", "google analytics 4"], "category": "tool"},
    "HubSpot":          {"aliases": ["hubspot"],                                 "category": "tool"},
    "Salesforce":       {"aliases": ["salesforce", "sfdc"],                     "category": "tool"},
    "Mailchimp":        {"aliases": ["mailchimp"],                               "category": "tool"},
    "Marketo":          {"aliases": ["marketo"],                                 "category": "tool"},
    "Klaviyo":          {"aliases": ["klaviyo"],                                 "category": "tool"},
    "SEO":              {"aliases": ["seo", "search engine optimization"],       "category": "domain"},
    "SEM":              {"aliases": ["sem", "search engine marketing", "paid search", "google ads", "ppc"], "category": "domain"},
    "Content Marketing":{"aliases": ["content marketing", "content strategy", "copywriting", "content creation"], "category": "domain"},
    "Social Media Marketing": {"aliases": ["social media marketing", "social media management", "instagram", "linkedin marketing", "tiktok"], "category": "domain"},
    "Email Marketing":  {"aliases": ["email marketing", "email campaigns", "drip campaigns"], "category": "domain"},
    "Performance Marketing": {"aliases": ["performance marketing", "growth marketing", "paid media", "paid advertising"], "category": "domain"},
    "Brand Marketing":  {"aliases": ["brand marketing", "brand strategy", "brand management"], "category": "domain"},
    "Product Marketing":{"aliases": ["product marketing", "go-to-market", "gtm strategy"], "category": "domain"},
    "Marketing Analytics": {"aliases": ["marketing analytics", "campaign analytics", "attribution"], "category": "domain"},
    "Conversion Optimization": {"aliases": ["conversion rate optimization", "cro", "landing page optimization"], "category": "domain"},

    # ════════════════════════════════════════════════════════
    # SALES TOOLS & SKILLS
    # ════════════════════════════════════════════════════════
    "CRM":              {"aliases": ["crm", "customer relationship management"], "category": "tool"},
    "Outreach":         {"aliases": ["outreach.io", "salesloft"],               "category": "tool"},
    "ZoomInfo":         {"aliases": ["zoominfo"],                                "category": "tool"},
    "B2B Sales":        {"aliases": ["b2b sales", "enterprise sales", "saas sales", "solution selling"], "category": "domain"},
    "B2C Sales":        {"aliases": ["b2c sales", "retail sales", "consumer sales"], "category": "domain"},
    "Account Management": {"aliases": ["account management", "account executive", "key account"], "category": "domain"},
    "Pipeline Management": {"aliases": ["pipeline management", "sales pipeline", "forecasting"], "category": "domain"},
    "Cold Outreach":    {"aliases": ["cold outreach", "cold calling", "cold email", "prospecting"], "category": "domain"},
    "Negotiation":      {"aliases": ["negotiation", "contract negotiation", "deal closing"], "category": "soft"},
    "Sales Enablement": {"aliases": ["sales enablement"],                        "category": "domain"},
    "Customer Success": {"aliases": ["customer success", "customer retention", "churn reduction"], "category": "domain"},
    "Revenue Operations": {"aliases": ["revenue operations", "revops"],         "category": "domain"},

    # ════════════════════════════════════════════════════════
    # FINANCE TOOLS & SKILLS
    # ════════════════════════════════════════════════════════
    "Excel":            {"aliases": ["excel", "microsoft excel", "advanced excel", "vlookup", "pivot tables"], "category": "tool"},
    "QuickBooks":       {"aliases": ["quickbooks", "quickbooks online"],         "category": "tool"},
    "NetSuite":         {"aliases": ["netsuite", "oracle netsuite"],             "category": "tool"},
    "SAP":              {"aliases": ["sap", "sap fi", "sap co"],                "category": "tool"},
    "Xero":             {"aliases": ["xero"],                                    "category": "tool"},
    "Tableau":          {"aliases": ["tableau"],                                 "category": "tool"},
    "Power BI":         {"aliases": ["power bi", "powerbi"],                     "category": "tool"},
    "Financial Modeling": {"aliases": ["financial modeling", "financial model", "dcf", "discounted cash flow", "lbo", "valuation"], "category": "domain"},
    "Financial Analysis": {"aliases": ["financial analysis", "financial reporting", "variance analysis"], "category": "domain"},
    "FP&A":             {"aliases": ["fp&a", "financial planning", "financial planning and analysis", "budgeting", "forecasting"], "category": "domain"},
    "GAAP":             {"aliases": ["gaap", "us gaap", "ifrs", "accounting standards"], "category": "domain"},
    "Accounting":       {"aliases": ["accounting", "bookkeeping", "general ledger", "accounts payable", "accounts receivable"], "category": "domain"},
    "Audit":            {"aliases": ["audit", "internal audit", "external audit", "sox", "sarbanes-oxley"], "category": "domain"},
    "Tax":              {"aliases": ["tax", "tax compliance", "corporate tax", "tax planning"], "category": "domain"},
    "Risk Management":  {"aliases": ["risk management", "credit risk", "market risk", "operational risk"], "category": "domain"},
    "Investment Analysis": {"aliases": ["investment analysis", "portfolio management", "asset management", "equity research"], "category": "domain"},
    "Treasury":         {"aliases": ["treasury", "cash management", "liquidity management"], "category": "domain"},

    # ════════════════════════════════════════════════════════
    # HR TOOLS & SKILLS
    # ════════════════════════════════════════════════════════
    "Workday":          {"aliases": ["workday"],                                 "category": "tool"},
    "BambooHR":         {"aliases": ["bamboohr", "bamboo hr"],                  "category": "tool"},
    "Greenhouse":       {"aliases": ["greenhouse"],                              "category": "tool"},
    "Lever":            {"aliases": ["lever"],                                   "category": "tool"},
    "ATS":              {"aliases": ["applicant tracking system", " ats ", "ats software"], "category": "tool"},
    "HRIS":             {"aliases": ["hris", "hr information system", "hr systems"], "category": "tool"},
    "Recruiting":       {"aliases": ["recruiting", "talent acquisition", "full-cycle recruiting", "sourcing", "headhunting"], "category": "domain"},
    "Onboarding":       {"aliases": ["onboarding", "employee onboarding"],      "category": "domain"},
    "Performance Management": {"aliases": ["performance management", "performance reviews", "okrs", "360 feedback"], "category": "domain"},
    "Learning & Development": {"aliases": ["learning and development", "l&d", "training and development", "employee training"], "category": "domain"},
    "Compensation & Benefits": {"aliases": ["compensation", "benefits", "total rewards", "compensation benchmarking"], "category": "domain"},
    "Employee Relations": {"aliases": ["employee relations", "employee engagement", "people operations"], "category": "domain"},
    "HR Compliance":    {"aliases": ["hr compliance", "labor law", "employment law", "eeoc"], "category": "domain"},
    "DEI":              {"aliases": ["dei", "diversity equity inclusion", "diversity and inclusion"], "category": "domain"},
    "Workforce Planning": {"aliases": ["workforce planning", "headcount planning", "org design"], "category": "domain"},

    # ════════════════════════════════════════════════════════
    # OPERATIONS / PROJECT MANAGEMENT
    # ════════════════════════════════════════════════════════
    "JIRA":             {"aliases": ["jira", "atlassian jira"],                  "category": "tool"},
    "Asana":            {"aliases": ["asana"],                                   "category": "tool"},
    "Monday.com":       {"aliases": ["monday.com", "monday"],                   "category": "tool"},
    "Confluence":       {"aliases": ["confluence"],                              "category": "tool"},
    "Notion":           {"aliases": ["notion"],                                  "category": "tool"},
    "Slack":            {"aliases": ["slack"],                                   "category": "tool"},
    "Zendesk":          {"aliases": ["zendesk"],                                 "category": "tool"},
    "Intercom":         {"aliases": ["intercom"],                                "category": "tool"},
    "Project Management": {"aliases": ["project management", "program management", "pmp"], "category": "domain"},
    "Agile":            {"aliases": ["agile", "scrum", "sprint", "kanban"],     "category": "domain"},
    "Process Improvement": {"aliases": ["process improvement", "process optimization", "lean", "six sigma", "kaizen"], "category": "domain"},
    "Operations Management": {"aliases": ["operations management", "ops", "business operations"], "category": "domain"},
    "Supply Chain":     {"aliases": ["supply chain", "logistics", "procurement", "vendor management"], "category": "domain"},
    "Business Analysis": {"aliases": ["business analysis", "requirements gathering", "business requirements"], "category": "domain"},
    "Data-Driven Decision Making": {"aliases": ["data-driven", "kpi", "metrics", "dashboards", "reporting"], "category": "domain"},
    "Change Management": {"aliases": ["change management", "organizational change"], "category": "domain"},
    "Cross-functional Collaboration": {"aliases": ["cross-functional", "cross functional collaboration"], "category": "soft"},

    # ════════════════════════════════════════════════════════
    # PRODUCT MANAGEMENT
    # ════════════════════════════════════════════════════════
    "Product Strategy": {"aliases": ["product strategy", "product vision"],     "category": "soft"},
    "Roadmapping":      {"aliases": ["roadmapping", "roadmap", "product roadmap"], "category": "soft"},
    "Stakeholder Management": {"aliases": ["stakeholder management", "stakeholders"], "category": "soft"},
    "User Research":    {"aliases": ["user research", "ux research", "user interviews", "usability testing"], "category": "soft"},
    "Product Analytics": {"aliases": ["product analytics", "mixpanel", "amplitude", "pendo"], "category": "tool"},
    "Product Launches": {"aliases": ["product launch", "go-to-market launch", "feature launch"], "category": "domain"},
    "Prioritization":   {"aliases": ["prioritization", "backlog", "backlog management"], "category": "domain"},

    # ════════════════════════════════════════════════════════
    # UNIVERSAL SOFT SKILLS
    # ════════════════════════════════════════════════════════
    "Communication":    {"aliases": ["communication skills", "written communication", "verbal communication", "presentation skills"], "category": "soft"},
    "Leadership":       {"aliases": ["leadership", "team leadership", "people management", "managing teams"], "category": "soft"},
    "Problem Solving":  {"aliases": ["problem solving", "critical thinking", "analytical thinking"], "category": "soft"},
    "Collaboration":    {"aliases": ["collaboration", "teamwork", "team player"], "category": "soft"},
    "Time Management":  {"aliases": ["time management", "prioritization", "multitasking"], "category": "soft"},
    "Data Analysis":    {"aliases": ["data analysis", "analytics"],              "category": "domain"},
    "Microsoft Office": {"aliases": ["microsoft office", "ms office", "word", "powerpoint"], "category": "tool"},
    "Google Workspace": {"aliases": ["google workspace", "google docs", "google sheets", "g suite"], "category": "tool"},
}

# Required-vs-preferred signal words
_REQUIRED_SIGNALS = re.compile(
    r"required|must have|must-have|you (will|should|must) (have|know|be)|"
    r"strong (background|experience|knowledge|understanding|proficiency) in|"
    r"expertise in|proven experience with",
    re.I,
)
_PREFERRED_SIGNALS = re.compile(
    r"nice to have|nice-to-have|preferred|bonus|plus|familiarity|exposure to|"
    r"ideally|experience with .{0,30} is a (plus|bonus)",
    re.I,
)


@dataclass
class ExtractedSkill:
    skill_name: str
    skill_category: str
    skill_raw: str
    is_required: bool
    extraction_method: str = "rule_based"
    confidence_score: float = 1.0


def extract_skills(description: str) -> list[ExtractedSkill]:
    """
    Extract skills from a job description using the rule-based taxonomy.
    Returns a deduplicated list of ExtractedSkill objects.
    """
    lower_desc = description.lower()
    found: dict[str, ExtractedSkill] = {}

    for canonical, meta in SKILLS_TAXONOMY.items():
        for alias in meta["aliases"]:
            # word-boundary aware search
            pattern = re.compile(r"(?<![a-z])" + re.escape(alias.lower()) + r"(?![a-z])")
            match = pattern.search(lower_desc)
            if match:
                if canonical in found:
                    break  # already found via another alias

                # Determine required vs preferred from surrounding context
                span_start = max(0, match.start() - 200)
                span_end   = min(len(lower_desc), match.end() + 200)
                context    = lower_desc[span_start:span_end]

                # Determine required/preferred from the section header BEFORE the match.
                # Only look backwards (up to 200 chars) — the nearest preceding signal
                # wins, as it defines the section the skill belongs to.
                match_pos  = match.start() - span_start  # position in context
                pre_context = context[:match_pos]         # text before the match

                pref_m = _PREFERRED_SIGNALS.search(pre_context)
                req_m  = _REQUIRED_SIGNALS.search(pre_context)

                # Use the signal that appears latest (closest to the skill)
                pref_pos = pref_m.end() if pref_m else -1
                req_pos  = req_m.end()  if req_m  else -1

                if pref_pos > req_pos:
                    is_required = False
                    confidence  = 0.85
                elif req_pos > pref_pos:
                    is_required = True
                    confidence  = 0.95
                else:
                    is_required = True   # default: assume required
                    confidence  = 0.80

                found[canonical] = ExtractedSkill(
                    skill_name=canonical,
                    skill_category=meta["category"],
                    skill_raw=match.group(),
                    is_required=is_required,
                    confidence_score=confidence,
                )
                break

    return list(found.values())
