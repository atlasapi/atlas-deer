package org.atlasapi.generation;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.meta.annotations.ProducesType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;


@ProducesType(type = TopLevelClass.class)
@Controller
public class DummyController {

    @RequestMapping("i'm/a/path")
    public void serveThings(HttpServletRequest request, HttpServletResponse response) {
        // do nothing
    }
}
