package edu.yu.velocitytrading.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class ExchangeServiceAdvice {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeServiceAdvice.class);
    
    @ExceptionHandler(QuoteNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String quoteNotFound(QuoteNotFoundException ex) {
        logger.error("Order on invalid symbol", ex);
        return ex.getMessage();
    }

    @ExceptionHandler(OrderValidationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String badOrder(OrderValidationException ex) {
        logger.error("Order is invalid", ex);
        return ex.getMessage();
    }
}
