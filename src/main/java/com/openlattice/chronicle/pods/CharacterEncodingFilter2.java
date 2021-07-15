package com.openlattice.chronicle.controllers.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@Component
@Order( Ordered.HIGHEST_PRECEDENCE )
public class CharacterEncodingFilter2 implements Filter {
    protected String encoding = StandardCharsets.UTF_8.toString();

    protected static final Logger logger = LoggerFactory.getLogger( CharacterEncodingFilter2.class );

    @Override public void init( FilterConfig filterConfig ) {
        logger.info( "initializing character encoding filter" );
    }

    @Override public void doFilter(
            ServletRequest request, ServletResponse response, FilterChain chain ) throws IOException, ServletException {
        // request.setCharacterEncoding( encoding );

        HttpServletRequest req = (HttpServletRequest) request;
        logger.info( "Request url is {}", request.getRemoteHost());

        request.setCharacterEncoding( "UTF-8" );
        chain.doFilter( request, response );
        logger.info( request.getCharacterEncoding() );
    }

    @Override public void destroy() {

    }
}
