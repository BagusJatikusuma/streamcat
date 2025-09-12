<?php
if (isset($_GET['z'])) {
    $z = $_GET['z'];

    // Set cookie with domain `.example.com` so it's valid for all subdomains
    // NOTE: Replace 'example.com' with your actual domain
    setcookie(
        "z",          // cookie name
        $z,                  // cookie value
        time() + (3600 * 24 * 3),       // expires in 3 days
        "/",                 // available across the entire site
        ".salsabilaschool.sch.id",      // domain (share across subdomains)
        true,                // secure (HTTPS only)
        true                 // HttpOnly (not accessible via JS)
    );
}
?>
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset='utf-8'>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>SmartPSB | Backoffice - Login</title>
    <link href="/css/carbon.css" rel="stylesheet">
    <link href="/css/output_backoffice.css" rel="stylesheet">
    <link href="/css/index.css" rel="stylesheet">
  </head>
  <body>
    <noscript>
      smart-psb is a JavaScript app. Please enable JavaScript to continue.
    </noscript>
    <div id="app"></div>
    <script>window.HOST = "https://pmb-backoffice.salsabilaschool.sch.id/api";</script>
    <script src="/js/compiled/app_backoffice_v003.js"></script>
  </body>
</html>