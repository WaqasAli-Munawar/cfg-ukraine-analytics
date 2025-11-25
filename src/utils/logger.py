"""
Logging configuration using structlog
"""
import structlog
import logging
import sys


def setup_logging(log_level: str = "INFO"):
    """Configure structured logging"""
    
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None):
    """Get a logger instance"""
    setup_logging()
    return structlog.get_logger(name)


if __name__ == "__main__":
    logger = get_logger("test")
    logger.info("✅ Logger is working!", status="success")