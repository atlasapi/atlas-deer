package org.atlasapi.meta.annotations;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Hey, this is some controller level description of the Content controller
 * 
 * @author Oliver Hall (oli@metabroadcast.com)
 *
 */
@ProducesType(type = Content.class)
@Controller
@RequestMapping("/4/content")
public class ContentController {

	
	@RequestMapping(value={ "/{cid}.*", "/{cid}", ".*", "" }, method = GET)
    public void writeContent(HttpServletRequest request, HttpServletResponse response) {
		// dummy method - no-op
	}
    
}
