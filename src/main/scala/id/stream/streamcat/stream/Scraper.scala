package id.stream.streamcat.stream

import cats.effect.*
import cats.syntax.all.*

import com.microsoft.playwright.Browser
import com.microsoft.playwright.Playwright
import com.microsoft.playwright.BrowserType
import java.util.ArrayList

//you should know that all operation in this class is not thread safe
//so its recommended to have instance per each fiber
class Scraper[F[_]: Sync](browser: Browser) {

  def getBrowser: Browser = browser

  def getTitle(url: String): F[String] =
    for
      page  <- Sync[F].blocking(browser.newPage())
      title <- Sync[F].blocking {
        page.navigate(url)
        page.title()
      }
    yield title

}

object Scraper:

  def make[F[_]: Sync]: Resource[F, Scraper[F]] =
    def acq =
      for
        playwright  <- Sync[F].delay(Playwright.create())
        //this will headless false to bypass some sites that blocks headless browser
        //you should run this program with xvfb-run on linux
        opts        =  new BrowserType.LaunchOptions().setHeadless(false)
                            .setArgs{
                              val list = new ArrayList[String]()
                              list.add("--disable-http2")
                              list.add("--disable-features=NetworkService")
                              list
                            }
        browser     <- Sync[F].blocking(playwright.chromium().launch(opts))
      yield Scraper(browser)
    
    def release(scraper: Scraper[F]): F[Unit] =
      Sync[F].blocking(scraper.getBrowser.close())

    Resource.make(acq)(release)
