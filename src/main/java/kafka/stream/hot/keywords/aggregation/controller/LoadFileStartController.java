package kafka.stream.hot.keywords.aggregation.controller;

import kafka.stream.hot.keywords.aggregation.service.MessagingAgent;
import kafka.stream.hot.keywords.aggregation.service.WordsProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/hotkeywords")
public class LoadFileStartController {
    private final WordsProducer wordsProducer;
    /*
       GET
       http://localhost:8091/hotkeywords/search
     */
    @GetMapping("/search")
    public String getHotKeywords() {
         MessagingAgent.getMessagingAgent().setStart();
         String result="";
         wordsProducer.uploadTestFileSend();
         while(true) {
             try {
                 Thread.sleep(1000);
             } catch (InterruptedException e) {}
             if (MessagingAgent.getStartStopFlag().equalsIgnoreCase("stop")) {
                 result = MessagingAgent.getResult();
                 break;
             }
         }
         return result;
    }
    /*
          GET
          http://localhost:8091/hotkeywords/searchbyfile/mediation.txt
          http://localhost:8091/hotkeywords/searchbyfile/resume.txt
          http://localhost:8091/hotkeywords/searchbyfile/politics.txt
          http://localhost:8091/hotkeywords/searchbyfile/description.txt
        */
    @GetMapping("/searchbyfile/{testFile}")
    public String getHotKeywordsByTestFile(@PathVariable("testFile") String fileName) {
        MessagingAgent.getMessagingAgent().setStart();
        String result="";
        wordsProducer.uploadTestFileSend(fileName);
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
            if (MessagingAgent.getStartStopFlag().equalsIgnoreCase("stop")) {
                result = MessagingAgent.getResult();
                break;
            }
        }
        return result;
    }
}
