[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![License][license-shield]][license-url]

<a name="readme-top"></a>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/lostsidewalk/newsgears-publisher">
    <img src="images/logo.png" alt="Logo" width="144" height="144" style="box-shadow: 2px 2px 2px rgba(64,64,64,0.7)">
  </a>

<h3 align="center">NewsGears RSS</h3>
 <p align="center">
    <a href="https://github.com/lostsidewalk/newsgears-publisher/issues">Report Bug</a>
    ·
    <a href="https://github.com/lostsidewalk/newsgears-publisher/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>


## What Is This

This repository contains the publisher.  The publisher is a back-end component that runs on demand to produce feeds from staging post entities.  The publisher delegates the task of producing artifacts of specific formats, such as RSS, ATOM, and JSON, to packages providing classes that implement the Publisher interface.

For more information about NewsGears, and RSS in general, see the parent project repository at: https://www.github.com/lostsidewalk/newsgears-app.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


### Built With

newsgears-publisher is a Java 19 library package.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

```
dependencies {
    api 'com.lostsidewalk.newsgears:newsgears-publisher'
}
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


See the [open issues](https://github.com/lostsidewalk/newsgears-publisher/issues) for a full list of known issues/proposed features.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".


1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/newFeature`)
3. Commit your Changes (`git commit -m 'Add some newFeature'`)
4. Push to the Branch (`git push origin feature/newFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the GPL V3 License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTACT -->
## Contact

meh - [@lostsidewalkllc](https://twitter.com/lostsidewalkllc) - meh@lostsidewalk.com

Project Link: [https://github.com/lostsidewalk/newsgears-publisher](https://github.com/lostsidewalk/newsgears-publisher)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [Project ROME](https://github.com/rometools)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/lostsidewalk/newsgears-publisher.svg?style=for-the-badge
[contributors-url]: https://github.com/lostsidewalk/newsgears-publisher/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/lostsidewalk/newsgears-publisher.svg?style=for-the-badge
[forks-url]: https://github.com/lostsidewalk/newsgears-publisher/network/members
[stars-shield]: https://img.shields.io/github/stars/lostsidewalk/newsgears-publisher.svg?style=for-the-badge
[stars-url]: https://github.com/lostsidewalk/newsgears-publisher/stargazers
[issues-shield]: https://img.shields.io/github/issues/lostsidewalk/newsgears-publisher.svg?style=for-the-badge
[issues-url]: https://github.com/lostsidewalk/newsgears-publisher/issues
[license-shield]: https://img.shields.io/github/license/lostsidewalk/newsgears-publisher.svg?style=for-the-badge
[license-url]: https://github.com/lostsidewalk/newsgears-publisher/blob/master/LICENSE
