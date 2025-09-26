// ===== MAIN JAVASCRIPT FOR EPA DATA MANAGEMENT WEBSITE =====

document.addEventListener('DOMContentLoaded', function() {
    // Initialize all components
    initNavigation();
    initScrollEffects();
    initAnimations();
    initInteractiveElements();
    initPerformanceTracking();
});

// ===== NAVIGATION =====
function initNavigation() {
    const navbar = document.getElementById('navbar');
    const navToggle = document.getElementById('nav-toggle');
    const navMenu = document.getElementById('nav-menu');
    const navLinks = document.querySelectorAll('.nav-link');

    // Mobile menu toggle
    navToggle.addEventListener('click', function() {
        navMenu.classList.toggle('active');
        navToggle.classList.toggle('active');
    });

    // Close mobile menu when clicking on links
    navLinks.forEach(link => {
        link.addEventListener('click', function() {
            navMenu.classList.remove('active');
            navToggle.classList.remove('active');
        });
    });

    // Navbar scroll effect
    let lastScrollTop = 0;
    window.addEventListener('scroll', function() {
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        
        // Add scrolled class for styling
        if (scrollTop > 50) {
            navbar.classList.add('scrolled');
        } else {
            navbar.classList.remove('scrolled');
        }

        // Hide/show navbar on scroll
        if (scrollTop > lastScrollTop && scrollTop > 100) {
            navbar.style.transform = 'translateY(-100%)';
        } else {
            navbar.style.transform = 'translateY(0)';
        }
        lastScrollTop = scrollTop;
    });

    // Smooth scroll for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
}

// ===== SCROLL EFFECTS =====
function initScrollEffects() {
    // Intersection Observer for fade-in animations
    const observerOptions = {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    };

    const observer = new IntersectionObserver(function(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('animate-in');
                
                // Add staggered animation for grid items
                if (entry.target.classList.contains('problem-grid') || 
                    entry.target.classList.contains('achievements-grid') ||
                    entry.target.classList.contains('tech-stack')) {
                    animateGridItems(entry.target);
                }
            }
        });
    }, observerOptions);

    // Observe elements for animation
    const animateElements = document.querySelectorAll('.section, .problem-card, .achievement-card, .tech-category');
    animateElements.forEach(el => observer.observe(el));

    // Parallax effect for hero section
    const hero = document.querySelector('.hero');
    if (hero) {
        window.addEventListener('scroll', function() {
            const scrolled = window.pageYOffset;
            const rate = scrolled * -0.5;
            hero.style.transform = `translateY(${rate}px)`;
        });
    }
}

// ===== ANIMATIONS =====
function initAnimations() {
    // Counter animation for stats
    const counters = document.querySelectorAll('.stat-number, .achievement-number');
    const counterObserver = new IntersectionObserver(function(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                animateCounter(entry.target);
                counterObserver.unobserve(entry.target);
            }
        });
    });

    counters.forEach(counter => counterObserver.observe(counter));

    // Progress bars animation
    const progressBars = document.querySelectorAll('.progress-bar');
    const progressObserver = new IntersectionObserver(function(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                animateProgressBar(entry.target);
                progressObserver.unobserve(entry.target);
            }
        });
    });

    progressBars.forEach(bar => progressObserver.observe(bar));
}

// ===== INTERACTIVE ELEMENTS =====
function initInteractiveElements() {
    // Card hover effects
    const cards = document.querySelectorAll('.problem-card, .achievement-card, .tech-category');
    cards.forEach(card => {
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-5px)';
        });
        
        card.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(0)';
        });
    });

    // Button click effects
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach(button => {
        button.addEventListener('click', function(e) {
            // Create ripple effect
            const ripple = document.createElement('span');
            const rect = this.getBoundingClientRect();
            const size = Math.max(rect.width, rect.height);
            const x = e.clientX - rect.left - size / 2;
            const y = e.clientY - rect.top - size / 2;
            
            ripple.style.width = ripple.style.height = size + 'px';
            ripple.style.left = x + 'px';
            ripple.style.top = y + 'px';
            ripple.classList.add('ripple');
            
            this.appendChild(ripple);
            
            setTimeout(() => {
                ripple.remove();
            }, 600);
        });
    });

    // Flow step interactions
    const flowSteps = document.querySelectorAll('.flow-step');
    flowSteps.forEach((step, index) => {
        step.addEventListener('click', function() {
            showStepDetails(index + 1);
        });
    });

    // Tech item tooltips
    const techItems = document.querySelectorAll('.tech-item');
    techItems.forEach(item => {
        item.addEventListener('mouseenter', function() {
            showTechTooltip(this);
        });
        
        item.addEventListener('mouseleave', function() {
            hideTechTooltip();
        });
    });
}

// ===== PERFORMANCE TRACKING =====
function initPerformanceTracking() {
    // Track page load time
    window.addEventListener('load', function() {
        const loadTime = performance.now();
        console.log(`Page loaded in ${loadTime.toFixed(2)}ms`);
        
        // Send analytics if available
        if (typeof gtag !== 'undefined') {
            gtag('event', 'page_load_time', {
                value: Math.round(loadTime)
            });
        }
    });

    // Track scroll depth
    let maxScroll = 0;
    window.addEventListener('scroll', function() {
        const scrollPercent = Math.round(
            (window.scrollY / (document.documentElement.scrollHeight - window.innerHeight)) * 100
        );
        
        if (scrollPercent > maxScroll) {
            maxScroll = scrollPercent;
            
            // Track milestone scrolls
            if (maxScroll >= 25 && maxScroll < 50) {
                trackEvent('scroll_depth', '25_percent');
            } else if (maxScroll >= 50 && maxScroll < 75) {
                trackEvent('scroll_depth', '50_percent');
            } else if (maxScroll >= 75 && maxScroll < 100) {
                trackEvent('scroll_depth', '75_percent');
            } else if (maxScroll >= 100) {
                trackEvent('scroll_depth', '100_percent');
            }
        }
    });

    // Track button clicks
    document.querySelectorAll('.btn').forEach(button => {
        button.addEventListener('click', function() {
            const buttonText = this.textContent.trim();
            const buttonHref = this.getAttribute('href');
            trackEvent('button_click', buttonText, buttonHref);
        });
    });
}

// ===== HELPER FUNCTIONS =====

function animateGridItems(grid) {
    const items = grid.children;
    Array.from(items).forEach((item, index) => {
        setTimeout(() => {
            item.classList.add('animate-in');
        }, index * 100);
    });
}

function animateCounter(element) {
    const target = parseInt(element.textContent.replace(/[^\d]/g, ''));
    const duration = 2000;
    const increment = target / (duration / 16);
    let current = 0;
    
    const timer = setInterval(() => {
        current += increment;
        if (current >= target) {
            current = target;
            clearInterval(timer);
        }
        
        // Format the number based on original text
        const originalText = element.textContent;
        if (originalText.includes('%')) {
            element.textContent = Math.floor(current) + '%';
        } else if (originalText.includes('K')) {
            element.textContent = Math.floor(current) + 'K+';
        } else if (originalText.includes('M')) {
            element.textContent = (current / 1000000).toFixed(1) + 'M+';
        } else {
            element.textContent = Math.floor(current).toLocaleString();
        }
    }, 16);
}

function animateProgressBar(progressBar) {
    const targetWidth = progressBar.getAttribute('data-width') || '100%';
    progressBar.style.width = '0%';
    
    setTimeout(() => {
        progressBar.style.width = targetWidth;
    }, 100);
}

function showStepDetails(stepNumber) {
    const stepDetails = {
        1: {
            title: 'Extract Phase',
            description: 'Memory-efficient processing of EPA CSV files using pandas chunking with configurable sizes.',
            metrics: ['50K+ records/second', '200MB memory per chunk', '<0.1% failure rate']
        },
        2: {
            title: 'Transform Phase',
            description: 'Comprehensive data cleaning, standardization, and business rule application.',
            metrics: ['96% data quality achieved', '2.5% duplicates removed', '12 quality indicators']
        },
        3: {
            title: 'Load Phase',
            description: 'Bulk PostgreSQL loading with UPSERT operations and transaction management.',
            metrics: ['100K+ records/second', '100% ACID compliance', '0% constraint violations']
        },
        4: {
            title: 'QA/QC Phase',
            description: 'Automated validation and anomaly detection using EPA standards.',
            metrics: ['42% error reduction', '8+ validation rules', '6+ detection methods']
        }
    };

    const details = stepDetails[stepNumber];
    if (details) {
        showModal(details);
    }
}

function showModal(content) {
    // Create modal if it doesn't exist
    let modal = document.getElementById('step-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'step-modal';
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content">
                <span class="modal-close">&times;</span>
                <h3 class="modal-title"></h3>
                <p class="modal-description"></p>
                <ul class="modal-metrics"></ul>
            </div>
        `;
        document.body.appendChild(modal);

        // Close modal functionality
        modal.querySelector('.modal-close').addEventListener('click', () => {
            modal.style.display = 'none';
        });

        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        });
    }

    // Update modal content
    modal.querySelector('.modal-title').textContent = content.title;
    modal.querySelector('.modal-description').textContent = content.description;
    
    const metricsList = modal.querySelector('.modal-metrics');
    metricsList.innerHTML = content.metrics.map(metric => `<li>${metric}</li>`).join('');

    // Show modal
    modal.style.display = 'flex';
}

function showTechTooltip(element) {
    const techName = element.querySelector('span').textContent;
    const tooltipContent = getTechTooltipContent(techName);
    
    if (tooltipContent) {
        // Create tooltip
        const tooltip = document.createElement('div');
        tooltip.className = 'tech-tooltip';
        tooltip.innerHTML = tooltipContent;
        
        // Position tooltip
        const rect = element.getBoundingClientRect();
        tooltip.style.position = 'fixed';
        tooltip.style.left = rect.right + 10 + 'px';
        tooltip.style.top = rect.top + 'px';
        tooltip.style.zIndex = '1000';
        
        document.body.appendChild(tooltip);
        
        // Store reference for cleanup
        element._tooltip = tooltip;
    }
}

function hideTechTooltip() {
    const tooltips = document.querySelectorAll('.tech-tooltip');
    tooltips.forEach(tooltip => tooltip.remove());
}

function getTechTooltipContent(techName) {
    const tooltips = {
        'Python 3.8+': 'High-performance programming language for data processing and ETL pipelines',
        'Pandas': 'Powerful data manipulation library with chunked processing capabilities',
        'PostgreSQL': 'Advanced open-source relational database with excellent performance',
        'SQLAlchemy': 'Python SQL toolkit and Object-Relational Mapping library',
        'NumPy': 'Fundamental package for scientific computing with optimized array operations',
        'SciPy': 'Scientific computing library with statistical and optimization functions',
        'Scikit-learn': 'Machine learning library used for anomaly detection algorithms',
        'Custom QA Engine': 'Proprietary validation and quality assurance automation system'
    };
    
    return tooltips[techName] || null;
}

function trackEvent(action, label, value = null) {
    // Google Analytics 4 tracking
    if (typeof gtag !== 'undefined') {
        gtag('event', action, {
            event_label: label,
            value: value
        });
    }
    
    // Console logging for development
    console.log('Event tracked:', { action, label, value });
}

function downloadTechnicalSpec() {
    // Create technical specification document
    const specs = {
        project: 'EPA Environmental Site Data Management & QA/QC Automation System',
        version: '1.0.0',
        date: new Date().toISOString().split('T')[0],
        achievements: {
            error_reduction: '42%',
            performance_improvement: '37%',
            throughput: '35,000+ records/second',
            success_rate: '99.1%'
        },
        architecture: {
            database: 'PostgreSQL 13+',
            processing: 'Python 3.8+ with Pandas',
            etl_approach: 'Staged ETL with chunked processing',
            qa_system: 'Custom validation engine with anomaly detection'
        },
        performance_metrics: {
            baseline_processing_time: '45.2 seconds',
            optimized_processing_time: '28.4 seconds',
            memory_usage_reduction: '33%',
            data_volume: '3.6GB+ (2.5M+ records)'
        },
        quality_improvements: {
            validation_rules: '8+ EPA standards-based rules',
            anomaly_detection: '6+ statistical methods',
            before_error_rate: '12.5%',
            after_error_rate: '7.2%'
        }
    };

    const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(specs, null, 2));
    const downloadAnchorNode = document.createElement('a');
    downloadAnchorNode.setAttribute("href", dataStr);
    downloadAnchorNode.setAttribute("download", "EPA_Data_System_Technical_Specifications.json");
    document.body.appendChild(downloadAnchorNode);
    downloadAnchorNode.click();
    downloadAnchorNode.remove();
    
    trackEvent('download', 'technical_specifications');
}

// ===== CSS ANIMATIONS FOR DYNAMIC ELEMENTS =====
const additionalStyles = `
<style>
.animate-in {
    opacity: 1 !important;
    transform: translateY(0) !important;
    transition: all 0.6s ease-out;
}

.section, .problem-card, .achievement-card, .tech-category {
    opacity: 0;
    transform: translateY(30px);
}

.ripple {
    position: absolute;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.3);
    pointer-events: none;
    animation: ripple-animation 0.6s ease-out;
}

@keyframes ripple-animation {
    to {
        transform: scale(2);
        opacity: 0;
    }
}

.modal {
    display: none;
    position: fixed;
    z-index: 10000;
    left: 0;
    top: 0;
    width: 100%;
    height: 100%;
    background-color: rgba(0, 0, 0, 0.5);
    align-items: center;
    justify-content: center;
}

.modal-content {
    background-color: white;
    padding: 2rem;
    border-radius: 1rem;
    max-width: 500px;
    width: 90%;
    position: relative;
    box-shadow: 0 20px 25px -5px rgb(0 0 0 / 0.1);
}

.modal-close {
    position: absolute;
    top: 1rem;
    right: 1.5rem;
    font-size: 2rem;
    cursor: pointer;
    color: #666;
}

.modal-close:hover {
    color: #000;
}

.modal-title {
    font-size: 1.5rem;
    font-weight: 600;
    margin-bottom: 1rem;
    color: var(--primary-color);
}

.modal-description {
    margin-bottom: 1.5rem;
    color: var(--gray-600);
    line-height: 1.6;
}

.modal-metrics {
    list-style: none;
}

.modal-metrics li {
    background: var(--gray-100);
    padding: 0.5rem 1rem;
    margin-bottom: 0.5rem;
    border-radius: 0.5rem;
    border-left: 4px solid var(--primary-color);
}

.tech-tooltip {
    background: var(--gray-900);
    color: white;
    padding: 0.75rem 1rem;
    border-radius: 0.5rem;
    font-size: 0.875rem;
    max-width: 250px;
    box-shadow: var(--shadow-lg);
    pointer-events: none;
}

.tech-tooltip::before {
    content: '';
    position: absolute;
    top: 50%;
    left: -5px;
    transform: translateY(-50%);
    border: 5px solid transparent;
    border-right-color: var(--gray-900);
}
</style>
`;

// Inject additional styles
document.head.insertAdjacentHTML('beforeend', additionalStyles);

// ===== SERVICE WORKER FOR PWA (OPTIONAL) =====
if ('serviceWorker' in navigator) {
    window.addEventListener('load', function() {
        navigator.serviceWorker.register('/sw.js').then(function(registration) {
            console.log('ServiceWorker registration successful');
        }, function(err) {
            console.log('ServiceWorker registration failed: ', err);
        });
    });
}
